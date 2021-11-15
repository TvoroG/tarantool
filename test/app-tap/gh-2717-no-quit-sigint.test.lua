#!/usr/bin/env tarantool

local tap = require('tap')
local popen = require('popen')
local process_timeout = require('process_timeout')
local fio = require('fio')
local clock = require('clock')

--
-- gh-2717: tarantool console quit on sigint
--
local file_read_timeout = 60.0
local file_read_interval = 0.2
local file_open_timeout = 60.0
local prompt = 'tarantool> '

local TARANTOOL_PATH = arg[-1]
local test = tap.test('gh-2717')

test:plan(3)

-- We set stdin to /dev/null to don't let the child process
-- manage terminal options. It may leave the terminal in an
-- inconsistent state and requires to call `reset` after the
-- test.
local ph = popen.new({'ERRINJ_STDIN_ISATTY=1 ' .. TARANTOOL_PATH .. ' -i 2>&1'}, {
    shell = true,
    setsid = true,
    group_signal = true,
    stdin = popen.opts.DEVNULL,
    stdout = popen.opts.PIPE,
    stderr = popen.opts.DEVNULL,
})
assert(ph, 'process is not up')

local start_time = clock.monotonic()
local deadline = 10.0

local output = ''
while output:find('tarantool> ^C\ntarantool>') == nil and clock.monotonic() - start_time < deadline do
    local data = ph:read({timeout = 0.1})
    if data ~= nil then
        output = output .. data
    end
    assert(ph:signal(popen.signal.SIGINT), 'can not send SIGINT')
end

assert(process_timeout.process_is_alive(ph.pid), 'process is dead')
test:unlike(ph:info().status.state, popen.state.EXITED, 'SIGINT doesn\'t kill tarantool in interactive mode ')
test:like(output, prompt .. '^C\n' .. prompt, 'Ctrl+C discards the input and calls the new prompt')

ph:close()

--
-- gh-2717: testing daemonized tarantool on signaling INT
--
local log_file = fio.abspath('tarantool.log')
local pid_file = fio.abspath('tarantool.pid')
local xlog_file = fio.abspath('00000000000000000000.xlog')
local snap_file = fio.abspath('00000000000000000000.snap')
local sock = fio.abspath('tarantool.sock')
local arg = ' -e \"box.cfg{pid_file=\''
            .. pid_file .. '\', log=\'' .. log_file .. '\', listen=\'unix/:' .. sock .. '\'}\"'

start_time = clock.monotonic()
deadline = 10.0

os.remove(log_file)
os.remove(pid_file)
os.remove(xlog_file)
os.remove(snap_file)

ph = popen.shell(TARANTOOL_PATH .. arg, 'r')

local f = process_timeout.open_with_timeout(log_file, file_open_timeout)
assert(f, 'error while opening ' .. log_file)

ph:signal(popen.signal.SIGINT)

local status = ph:wait(nil, popen.signal.SIGINT)
test:unlike(status.state, popen.state.ALIVE, 'SIGINT terminates tarantool in daemon mode ')

local data = ''
while data:match('got signal 2') == nil and clock.monotonic() - start_time < deadline do
    data = process_timeout.read_with_timeout(f,
            file_read_timeout,
            file_read_interval)
end
assert(data:match('got signal 2'), 'there is no one note about SIGINT')

f:close()
ph:close()
os.remove(log_file)
os.remove(pid_file)
os.remove(xlog_file)
os.remove(snap_file)
os.exit(test:check() and 0 or 1)
