local t = require('luatest')

local server = require('test.luatest_helpers.server')

local g = t.group()

g.before_all(function()
    local box_cfg = {
        work_dir = os.environ()['VARDIR'],
        memtx_memory = 512 * 1024 * 1024,
        memtx_max_tuple_size = 4 * 1024 * 1024,
        vinyl_read_threads = 2,
        vinyl_write_threads = 3,
        vinyl_memory = 512 * 1024 * 1024,
        vinyl_range_size = 1024 * 64,
        vinyl_page_size = 1024,
        vinyl_run_count_per_level = 1,
        vinyl_run_size_ratio = 2,
        vinyl_cache = 10240, -- 10kB
        vinyl_max_tuple_size = 1024 * 1024 * 6,
    }
    g.server = server:new({alias = 'master', box_cfg = box_cfg})
    g.server:start()
end)

g.before_each(function()
    g.server:exec(function()
        box.schema.space.create('test', {engine = 'vinyl'})
    end)
end)

g.after_each(function()
    g.server:exec(function() box.space.test:drop() end)
end)

local function dump_stmt_count(indexes)
    local dumped_count = 0
    for _, i in ipairs(indexes) do
        dumped_count = dumped_count +
            box.space.test.index[i]:stat().disk.dump.output.rows
    end
    return dumped_count
end

local function check_snapshot(result)
    t.assert_equals(g.server:exec(function() return box.snapshot() end), result)
end

local function check_insert(data)
    t.assert_equals(
        g.server:exec(function(d)
            return box.space.test:insert(d)
        end, {data}),
        data
    )
end

local function check_replace(data)
    t.assert_equals(
        g.server:exec(function(d)
            return box.space.test:replace(d)
        end, {data}),
        data
    )
end

local function check_update(data, expected)
    t.assert_equals(
        g.server:exec(function(...)
            return box.space.test:update(...)
        end, {unpack(data)}),
        expected
    )
end

local function check_select(index, expected)
    t.assert_equals(
        g.server:exec(function(i)
            return box.space.test.index[i]:select {}
        end, {index}),
        expected
    )
end

g.test_optimize_one_index = function()
    g.server:exec(function()
        box.space.test:create_index('primary', {run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('secondary',
            {parts = {5, 'unsigned'}, run_count_per_level = 20})
    end)

    check_snapshot('ok')

    local old_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary'}})

    check_insert({1, 2, 3, 4, 5})
    check_insert({2, 3, 4, 5, 6})
    check_insert({3, 4, 5, 6, 7})
    check_insert({4, 5, 6, 7, 8})
    check_snapshot('ok')

    local new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 8)

    -- Not optimized updates.

    -- Change secondary index field.
    check_update({1, {{'=', 5, 10}}}, {1, 2, 3, 4, 10})
    -- Need a snapshot after each operation to avoid purging some statements
    -- in vy_write_iterator during dump.
    check_snapshot('ok')

    -- Move range containing index field.
    check_update({1, {{'!', 4, 20}}}, {1, 2, 3, 20, 4, 10})
    check_snapshot('ok')

    -- Move range containing index field.
    check_update({1, {{'#', 3, 1}}}, {1, 2, 20, 4, 10})
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count, {{'primary', 'secondary'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 9)

    check_select('primary',
        {
            {1, 2, 20, 4, 10},
            {2, 3, 4, 5, 6},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8}
        }
    )
    check_select('secondary',
        {
            {2, 3, 4, 5, 6},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8},
            {1, 2, 20, 4, 10}
        }
    )

    -- Optimized updates.

    -- Change not indexed field.
    check_update({2, {{'=', 6, 10}}}, {2, 3, 4, 5, 6, 10})
    check_snapshot('ok')

    -- Move range that doesn't contain indexed fields.
    check_update({2, {{'!', 7, 20}}}, {2, 3, 4, 5, 6, 10, 20})
    check_snapshot('ok')

    check_update({2, {{'#', 6, 1}}}, {2, 3, 4, 5, 6, 20})
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count, {{'primary', 'secondary'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 3)

    check_select('primary',
        {
            {1, 2, 20, 4, 10},
            {2, 3, 4, 5, 6, 20},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8}
        }
    )
    check_select('secondary',
        {
            {2, 3, 4, 5, 6, 20},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8},
            {1, 2, 20, 4, 10}
        }
    )
end

g.test_optimize_two_indexes = function()
    g.server:exec(function()
        box.space.test:create_index('primary',
            {parts = {2, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('secondary',
            {parts = {4, 'unsigned', 3, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('third',
            {parts = {5, 'unsigned'}, run_count_per_level = 20})
    end)

    check_snapshot('ok')

    local old_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})

    check_insert({1, 2, 3, 4, 5})
    check_insert({2, 3, 4, 5, 6})
    check_insert({3, 4, 5, 6, 7})
    check_insert({4, 5, 6, 7, 8})
    check_snapshot('ok')

    local new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 12)

    -- Not optimized updates.

    -- Change all fields.
    check_update(
        {2, {{'+', 1, 10}, {'+', 3, 10}, {'+', 4, 10}, {'+', 5, 10}}},
        {11, 2, 13, 14, 15}
    )
    check_snapshot('ok')

    -- Move range containing all indexes.
    check_update({2, {{'!', 3, 20}}}, {11, 2, 20, 13, 14, 15})
    check_snapshot('ok')

    -- Change two cols but then move range with all indexed fields.
    check_update(
        {2, {{'=', 7, 100}, {'+', 5, 10}, {'#', 3, 1}}},
        {11, 2, 13, 24, 15, 100}
    )
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 15)

    check_select('primary',
        {
            {11, 2, 13, 24, 15, 100},
            {2, 3, 4, 5, 6},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8}
        }
    )
    check_select('secondary',
        {
            {2, 3, 4, 5, 6},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8},
            {11, 2, 13, 24, 15, 100}
        }
    )
    check_select('third',
        {
            {2, 3, 4, 5, 6},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8},
            {11, 2, 13, 24, 15, 100}
        }
    )

    -- Optimize one 'secondary' index update.

    -- Change only index 'third'.
    check_update(
        {3, {{'+', 1, 10}, {'-', 5, 2}, {'!', 6, 100}}},
        {12, 3, 4, 5, 4, 100}
    )
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 3)

    -- Optimize one 'third' index update.

    -- Change only index 'secondary'.
    check_update(
        {3, {{'=', 1, 20}, {'+', 3, 5}, {'=', 4, 30}, {'!', 6, 110}}},
        {20, 3, 9, 30, 4, 110, 100}
    )
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 3)

    -- Optimize both indexes.

    -- Not change any indexed fields.
    check_update({3, {{'+', 1, 10}, {'#', 6, 1}}}, {30, 3, 9, 30, 4, 100})
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 1)

    check_select('primary',
        {
            {11, 2, 13, 24, 15, 100},
            {30, 3, 9, 30, 4, 100},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8}
        }
    )
    check_select('secondary',
        {
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8},
            {11, 2, 13, 24, 15, 100},
            {30, 3, 9, 30, 4, 100}
        }
    )
    check_select('third',
        {
            {30, 3, 9, 30, 4, 100},
            {3, 4, 5, 6, 7},
            {4, 5, 6, 7, 8},
            {11, 2, 13, 24, 15, 100}
        }
    )
end

-- gh-1716: optimize UPDATE with field num > 64.
g.test_optimize_UPDATE_with_field_num_more_than_64 = function()
    g.server:exec(function()
        box.space.test:create_index('primary',
            {parts = {2, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('secondary',
            {parts = {4, 'unsigned', 3, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('third',
            {parts = {5, 'unsigned'}, run_count_per_level = 20})
    end)

    -- Create a big tuple.
    local long_tuple = {}
    for i = 1, 70 do long_tuple[i] = i end

    check_replace(long_tuple)
    check_snapshot('ok')

    -- Make update of not indexed field with pos > 64.
    local old_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    long_tuple[65] = 1000
    check_update({2, {{'=', 65, 1000}}}, long_tuple)
    check_snapshot('ok')

    -- Check only primary index to be changed.
    local new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 1)
    t.assert_equals(
        g.server:exec(function() return box.space.test:get {2}[65] end),
        1000
    )

    -- Try to optimize update with negative field numbers.

    check_update({2, {{'#', -65, 65}}}, {1, 2, 3, 4, 5})
    check_snapshot('ok')

    old_stmt_count = new_stmt_count
    new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    t.assert_equals(new_stmt_count - old_stmt_count, 1)

    check_select('primary', {{1, 2, 3, 4, 5}})
    check_select('secondary', {{1, 2, 3, 4, 5}})
    check_select('third', {{1, 2, 3, 4, 5}})

    check_replace({10, 20, 30, 40, 50})
    check_snapshot('ok')

    old_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})

    check_update({20, {{'=', -1, 500}}}, {10, 20, 30, 40, 500})
    check_snapshot('ok')

    new_stmt_count = g.server:exec(dump_stmt_count,
        {{'primary', 'secondary', 'third'}})
    -- 3 = REPLACE in 1 index and DELETE + REPLACE in 3 index.
    t.assert_equals(new_stmt_count - old_stmt_count, 3)

    check_select('primary', {{1, 2, 3, 4, 5}, {10, 20, 30, 40, 500}})
    check_select('secondary', {{1, 2, 3, 4, 5}, {10, 20, 30, 40, 500}})
    check_select('third', {{1, 2, 3, 4, 5}, {10, 20, 30, 40, 500}})
end

g.test_optimizes_update_does_not_skip_entire_key_during_dump = function()
    g.server:exec(function()
        box.space.test:create_index('primary',
            {parts = {2, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('secondary',
            {parts = {4, 'unsigned', 3, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('third',
            {parts = {5, 'unsigned'}, run_count_per_level = 20})
    end)

    check_replace({10, 100, 1000, 10000, 100000, 1000000})
    check_update({100, {{'=', 6, 1}}}, {10, 100, 1000, 10000, 100000, 1})

    local conn = require('net.box').self
    conn:call('box.begin')

    check_replace({20, 200, 2000, 20000, 200000, 2000000})
    check_update({200, {{'=', 6, 2}}}, {20, 200, 2000, 20000, 200000, 2})
    conn:call('box.commit')
    check_snapshot('ok')

    check_select('primary',
        {
            {10, 100, 1000, 10000, 100000, 1},
            {20, 200, 2000, 20000, 200000, 2}
        }
    )
    check_select('secondary',
        {
            {10, 100, 1000, 10000, 100000, 1},
            {20, 200, 2000, 20000, 200000, 2}
        }
    )
    check_select('third',
        {
            {10, 100, 1000, 10000, 100000, 1},
            {20, 200, 2000, 20000, 200000, 2}
        }
    )
end

-- gh-2980: key uniqueness is not checked if indexed fields are not updated.
g.test_key_uniqueness_not_checked_if_indexed_fields_not_updated = function()
    g.server:exec(function()
        box.space.test:create_index('primary',
            {parts = {2, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('secondary',
            {parts = {4, 'unsigned', 3, 'unsigned'}, run_count_per_level = 20})
    end)
    g.server:exec(function()
        box.space.test:create_index('third',
            {parts = {5, 'unsigned'}, run_count_per_level = 20})
    end)

    check_replace({1, 1, 1, 1, 1})

    local function get_lookups(lb)
        local ret = {}
        for i = 1, #lb do
            local info = box.space.test.index[i - 1]:stat()
            table.insert(ret, info.lookup - lb[i])
        end
        return ret
    end

    local lookups = g.server:exec(get_lookups, {{0, 0, 0}})

    -- Update field that is not indexed.
    check_update({1, {{'+', 1, 1}}}, {2, 1, 1, 1, 1})
    t.assert_equals(g.server:exec(get_lookups, {lookups}), {1, 0, 0})

    -- Update field indexed by space.index[1].
    check_update({1, {{'+', 3, 1}}}, {2, 1, 2, 1, 1})
    t.assert_equals(g.server:exec(get_lookups, {lookups}), {2, 1, 0})

    -- Update field indexed by space.index[2].
    check_update({1, {{'+', 5, 1}}}, {2, 1, 2, 1, 2})
    t.assert_equals(g.server:exec(get_lookups, {lookups}), {3, 1, 1})
end

-- gh-3607: phantom tuples in secondary index if UPDATE does not change key
-- fields.
g.test_no_phantom_tuples_in_secondary_index = function()
    g.server:exec(function() box.space.test:create_index('primary') end)
    g.server:exec(function()
        box.space.test:create_index('secondary',
            {parts = {2, 'unsigned'}, run_count_per_level = 10})
    end)

    check_insert({1, 10})
    -- Some padding to prevent last-level compaction (gh-3657).
    g.server:exec(function()
        for i = 1001, 1010 do box.space.test:replace {i, i} end
    end)
    check_snapshot('ok')

    check_update({1, {{'=', 2, 10}}}, {1, 10})
    g.server:exec(function() box.space.test:delete(1) end)
    check_snapshot('ok')

    -- Should be 12: INSERT{10, 1} and INSERT[1001..1010] in the first run
    -- plus DELETE{10, 1} in the second one.
    t.assert_equals(
        g.server:exec(function()
            return box.space.test.index.secondary:stat().rows
        end),
        12
    )

    check_insert({1, 20})
    check_select('secondary',
        {
            {1, 20},
            {1001, 1001},
            {1002, 1002},
            {1003, 1003},
            {1004, 1004},
            {1005, 1005},
            {1006, 1006},
            {1007, 1007},
            {1008, 1008},
            {1009, 1009},
            {1010, 1010}
        }
    )
end
