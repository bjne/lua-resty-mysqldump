local _M = { _VERSION = 0.1 }
local _D, SQL = {}

local mysql = require "resty.mysql"
local cjson = require "cjson.safe"

local concat = table.concat
local insert = table.insert
local format = string.format
local gsub   = string.gsub

local ERR = function(...)
    ngx.log(ngx.ERR, ...)
    return nil, concat({...})
end

local mysql_type = {
    [0] = [[concat("X'", HEX(%s), "'")]],
    {
        [0] = '%s',
        'bit', 'tinyint', 'smallint', 'mediumint', 'int', 'integer',
        'bigint', 'real', 'double', 'float', 'decimal', 'numeric',
    },
    {
        [0] = 'quote(%s)',
        'varchar', 'tinytext', 'text', 'mediumtext', 'longtext', 'datetime'
    }
}

setmetatable(mysql_type, { __index = function(t, data_type)
    local function return_function(fmt)
        return function(data) return format(fmt, data) end
    end

    for _,t in ipairs(t) do
        for _,dt in ipairs(t) do
            if dt == data_type then
                return return_function(t[0])
            end
        end
    end

    return return_function(t[0])
end})

function _D:show(what)
    return SQL[what] and (gsub(SQL[what], '%%%(([^)]+)%)', function(key)
        return self[key]
    end))
end

function _D:say(what)
    ngx.say(self:show(what))
end

function _D:get(what, field)
    local query = self:show(what)

    if query then
        self.db:set_compact_arrays(true)
        local res, err = self.db:query(query)
        if err then
            return ERR(err)
        end

        if res and res[1] then
            if field then
                return res[1][field]
            end

            return unpack(res[1])
        end
    end
end

function _D:rows(what, finalize)
    local query = self:show(what)

    if query then
        local bytes, err = self.db:send_query(query)

        local function iterate()
            local res, err, errcode, sqlstate = self.db:read_result(500)

            for _,row in ipairs(res or {}) do
                coroutine.yield(row)
            end

            return err == 'again' and iterate() or (finalize and finalize())
        end

        return bytes and coroutine.wrap(function() iterate() end)
    end
end

function _D:dump_rows()
    local insert_into = self:show('insert')
    local length, rows = #insert_into, {}

    local say_insert_into = function(row)
        if not row or length + #row >= 1024 * 1024 then
            ngx.say(insert_into .. "\n" .. concat(rows, ',\n') ..';')
            rows, length = {}, #insert_into
        elseif row then
            insert(rows, row)
            length = length + 1 + #row
        end
    end

    self:say('lock_tables')
    for row in self:rows('rows', say_insert_into) do
        say_insert_into('  (' .. concat(row, ',') ..')')
    end
    self:say('unlock_tables')
end

function _D:dump_tables(table_type, view_temp)
    local tables, table_number, table_name = {}, 0

    self.db:set_compact_arrays(false)
    self.table_type = table_type or 'BASE TABLE'
    for _table in self:rows('tables') do
        if table_name ~= _table.table_name then
            table_name = _table.table_name
            table_number = table_number + 1
            tables[table_number] = {
                table_rows = tonumber(_table.table_rows),
                table_name = _table.table_name
            }
        end

        tables[table_number][tonumber(_table.ordinal_position)] =
            table_type == 'VIEW'
            and format([[  `%s` tinyint NOT NULL]], _table.column_name)
            or mysql_type[_table.column_data_type](_table.column_name)
    end

    for _, _table in ipairs(tables) do
        self.table = _table

        if table_type == 'VIEW' then
            _table.create_view =
                view_temp == true and concat(_table, ',\n')
                or gsub(self:get('show_create_view', 2),
                '^CREATE (ALGORITHM=.-) (DEFINER=.-) VIEW (.+)$',
                '/*!50001 CREATE %1 */\n/*!50013 %2 */\n/*!50001 VIEW %3 */')
            self:say('create_view' .. (view_temp and '_tmp' or ''))
        else
            _table.create_table = self:get('show_create_table', 2)
            _table.columns = concat(_table, ',')
            self:say('create_table')

            if _table.table_rows > 0 then
                self:dump_rows()
            end
        end
    end
end

function _D:dump_routines()
    local routines = {}

    self.db:set_compact_arrays(false)
    for routine in self:rows('routines') do
        insert(routines, routine)
    end

    for _, routine in ipairs(routines) do
        self.routine = routine
        routine.create_routine = self:get('show_create_routine', 3)

        self:say('create_routine')
    end
end

function _D:dump_triggers()
    local triggers = {}

    self.db:set_compact_arrays(false)
    for trigger in self:rows('triggers') do
        insert(triggers, trigger)
    end

    for _, trigger in ipairs(triggers) do
        self.trigger = trigger
        trigger.create_trigger = gsub(self:get('show_create_trigger', 3),
            '^CREATE (DEFINER=.-) TRIGGER (.*)',
            '/*!50003 CREATE */ /*!50017 %1 */ /*!50003 TRIGGER %2 */')

        self:say('create_trigger')
    end
end

function _D:dump_events()
    -- TODO
end

function _D:dump_views(temporary)
    self:dump_tables('VIEW', temporary)
end

function _D:dump()
    self:say('BEGIN')
    self:dump_views(true)
    self:dump_tables()
    self:dump_routines()
    self:dump_triggers()
    self:dump_views()
    self:dump_events()
    self:say('END')
end

_M.new = function(hostname, database, username, password)
    local self = { database = database, db = mysql:new() }

    if not self.db then
        return ERR("failed to instantiate mysql")
    end

    self.db:set_timeout(1000)

    local ok, err, errcode, sqlstate = self.db:connect{
        host = hostname,
        port = 3306,
        database = database,
        user = username,
        password = password,
        charset = "utf8",
        max_packet_size = 1024 * 1024,
    }

    if not ok then
        return ERR("mysql: ", err, ": ", errcode, " ", sqlstate)
    end

    self = setmetatable(self, { __index = function(t, key)
        if _D[key] then
            return _D[key]
        end

        local value = t
        gsub(key, '([^.]+)', function(k)
            value = type(value) == 'table' and rawget(value, k)
        end)

        return value
    end})

    for variable in self:rows('variables') do
        self[variable.Variable_name] = variable.Value
    end

    return self
end

SQL = {
variables = [[
  show variables
 where variable_name rlike('^(character_set_|collation_).*')
]],
tables = [[
select t.table_name as table_name,
       t.table_rows as table_rows,
       c.column_name as column_name,
       c.column_type as column_type,
       c.data_type as column_data_type,
       c.ordinal_position as ordinal_position
  from information_schema.tables as t
  join information_schema.columns as c using(table_schema, table_name)
 where t.table_type='%(table_type)'
   and t.table_schema='%(database)'
]],
routines = [[
select routine_name,
       routine_comment,
       routine_type,
       security_type,
       sql_mode,
       definer,
       character_set_client,
       collation_connection,
       database_collation
  from information_schema.routines
 where routine_type IN('PROCEDURE', 'FUNCTION')
       and routine_schema='%(database)'
 order by routine_type, routine_name
]],
views = [[
select table_name as view_name
  from information_schema.tables
 where table_type='view'
   and table_schema='%(database)'
]],
events = [[
select event_name as event_name
  from information_schema.events
 where event_schema='%(database)'
]],
triggers = [[
select trigger_name,
       event_manipulation,
       event_object_table,
       action_orientation,
       action_timing,
       sql_mode,
       definer,
       character_set_client,
       collation_connection,
       database_collation
  from information_schema.triggers
 where trigger_schema='%(database)'
 order by action_order
]],
columns = [[
select column_name as column_name
  from information_schema.columns
 where table_schema='%(database)'
   and table_name='%(table)'
]],
show_create_table = [[
  show create table `%(table.table_name)`
]],
show_create_view = [[
  show create table `%(table.table_name)`
]],
show_create_routine = [[
  show create %(routine.routine_type) `%(routine.routine_name)`
]],
show_create_trigger = [[
  show create trigger `%(trigger.trigger_name)`
]],
rows = [[
select %(table.columns)
  from %(table.table_name)
]],
insert = [[
INSERT INTO `%(table.table_name)` VALUES
]],
lock_tables = [[

LOCK TABLES `%(table.table_name)` WRITE;
/*!40000 ALTER TABLE `%(table.table_name)` DISABLE KEYS */;
]],
unlock_tables = [[
/*!40000 ALTER TABLE `%(table.table_name)` ENABLE KEYS */;
UNLOCK TABLES;
]],
create_table = [[

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = %(character_set_connection) */;
DROP TABLE IF EXISTS `%(table.table_name)`;
%(table.create_table);
/*!40101 SET character_set_client = @saved_cs_client */;
]],
create_view_tmp = [[

DROP TABLE IF EXISTS `%(table.table_name)`;
/*!50001 DROP VIEW IF EXISTS `%(table.table_name)` */;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = %(character_set_client);
/*!50001 CREATE TABLE `%(table.table_name)` (
%(table.create_view)
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;
]],
create_view = [[

/*!50001 DROP TABLE IF EXISTS `%(table.table_name)` */;
/*!50001 DROP VIEW IF EXISTS `%(table.table_name)` */;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = %(character_set_client) */;
/*!50001 SET character_set_results     = %(character_set_results) */;
/*!50001 SET collation_connection      = %(collation_connection) */;
%(table.create_view);
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;
]],
create_routine = [[

/*!50003 DROP %(routine.routine_type) IF EXISTS `%(routine.routine_name)` */ ;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = %(character_set_connection) */ ;
/*!50003 SET character_set_results = %(character_set_results) */ ;
/*!50003 SET collation_connection  = %(collation_connection) */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '%(routine.sql_mode)' */ ;

DELIMITER ;;
%(routine.create_routine) ;;
DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
]],
create_trigger = [[

/*!50003 SET @saved_cs_client      = @@character_set_client  */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection  */ ;
/*!50003 SET character_set_client  = %(character_set_connection) */ ;
/*!50003 SET character_set_results = %(character_set_results) */ ;
/*!50003 SET collation_connection  = %(collation_connection) */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '%(trigger.sql_mode)' */ ;

DELIMITER ;;
%(trigger.create_trigger) ;;
DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode       */ ;
/*!50003 SET character_set_client  = @saved_cs_client      */ ;
/*!50003 SET character_set_results = @saved_cs_results     */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
]],
BEGIN = [[
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */ ;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */ ;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */ ;
/*!40101 SET NAMES utf8mb4 */ ;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */ ;
/*!40103 SET TIME_ZONE='+00:00' */ ;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */ ;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */ ;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */ ;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */ ;
]],
END = [[
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */ ;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */ ;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */ ;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */ ;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */ ;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */ ;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */ ;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */ ;
]]
}

for k, q in pairs(SQL) do
    SQL[k] = string.gsub(q, '\n%s*$', '')
end

return _M
