# taran

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

```erlang
Db = taran:connect(_ConnName = test).
{ok,test}

taran:insert(test, [1, <<"test_row">>]).
{ok, [1, <<"test_row">>]}

taran:select(test, [1]).
{ok, [[1, <<"test_row">>]]}

taran:eval(test, <<"return 'hello'">>).
{ok,<<"hello">>}

taran:connect_close(test).
ok
```

See taran.erl for more commands, options and defaults.
