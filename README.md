# taran


Erlang tarantool connector

INSTALL

add it to your rebar config

```erlang

{deps, [
    ....
    {taran, ".*", {git, "https://github.com/stofel/taran.git", {branch, "master"}}}
]}.
```

and add taran to your_project.app.src file

USAGE

Default host localhost, default port 3301

```erlang
{ok, Db} = taran:connect(_ConnName = test).
{ok,test}

taran:insert(Db, [1, <<"test_row">>]).
{ok, [1, <<"test_row">>]}

taran:select(Db, [1]).
{ok, [[1, <<"test_row">>]]}

taran:eval(Db, <<"return 'hello'">>).
{ok,<<"hello">>}

taran:connect_close(Db).
ok

taran:eval(Db, <<"return {['hello']={'hello'}}">>).
{ok,#{<<"hello">> => [<<"hello">>]}}
```

See taran.erl for more commands, options and defaults.
