# taran


Erlang tarantool 1.6/1.7 connector.

DESCRIPTION
1. Pools asynchronus connections (OTP supervisor based)
2. Auto reconnect
3. Erlang maps <-> Lua tables support


INSTALL

add it to your rebar config

```erlang

{deps, [
    ....
    {taran, ".*", {git, "https://github.com/stofel/taran.git", {branch, "master"}}}
]}.
```

and add taran to your_project.app.src file.

For tarantool 1.6 call function  support delete {d, 'TARANTOOL_V17_CALL'} from taran/rebar.conf

USAGE

```erlang
{ok, Db} = taran:connect(_ConnName = test).
{ok,test}
%% or
Args = #{host => "127.0.0.1",   %% Host (default "localhost")
         port => 3301,          %% Port (default 3301)
         user => <<"none">>,    %% User (default <<"none">>, use guest access)
         pass => <<"none">>     %% Pass (default <<"none">>, use guest access)
         cnum => 3},            %% CNum Number of open sokets for connect (default 3)
taran:connect(_ConnName = test, Args).
{ok,test}
%% or
taran:connect(_ConnName = test, #{port => 3311}).
{ok,test}

taran:connect_list().
[{test,<0.926.0>,supervisor,[taran_conns_sup]}]


%% Insert
taran:insert(Db, [1, <<"test_row">>]).
{ok, [1, <<"test_row">>]}
%% or
Args = #{space_id => 514}, %% SpaceId (default 0)
taran:insert(Db, [1, <<"test_row">>], Args).
{ok, [1, <<"test_row">>]}


%% Select
taran:select(Db, [1]).
{ok, [[1, <<"test_row">>]]}
%% or
Args = #{space_id => 0,           %% SpaceId 0 by default
         index_id => 0,           %% IndexId 0 by default
         limit    => 16#FFFFFFFF, %% Very big number by default
         offset   => 0,           %% No offset by default
         iterator => 0},          %% EQ by default (see tarantool doc or taran.erl for more iterators)
taran:select(Db, [1], Args).
{ok, [[1, <<"test_row">>]]}
 
%% Update
Op = [<<"=">>, 1, <<"test_row2">>]
taran:update(Db, [1], [Op]).
{ok, []}
%% or
Args = #{space_id => 0,           %% SpaceId 0 by default
         index_id => 0},          %% IndexId 0 by default
taran:update(Db, [1], [Op], Args).
{ok, []}

%% Upsert
Tuple = {3, <<"test_row3">>},}
Op = [<<"=">>, 1, <<"test_row3">>],
taran:upsert(Db, Tuple, [Op]) 
{ok, []}
%% or
Args = #{space_id => 0,           %% SpaceId 0 by default
         index_id => 0},          %% IndexId 0 by default
taran:upsert(Db, Tuple, [Op], Args).
{ok, []}


%% Other cmds

taran:eval(Db, <<"return 'hello'">>).
{ok,<<"hello">>}

taran:eval(Db, <<"return {['hello']={'hello'}}">>).
{ok,#{<<"hello">> => [<<"hello">>]}}

taran:connect_close(Db).
ok
```

See taran.erl for more commands, options and defaults.

