-module(taran).

-export([
    connect/0, connect/1, connect/2,
    connect_close/1, connect_close_all/0,
    connect_list/0
  ]).


-export([
    select/1, select/2, select/3,
    insert/2, insert/3,
    replace/2, replace/3,
    update/3, update/4,
    delete/2, delete/3,
    call/1, call/2, call/3,
    eval/1, eval/2, eval/3,
    upsert/3, upsert/4
  ]).


%% http://tarantool.org/doc/dev_guide/box-protocol.html#iproto-protocol
-define(IPROTO_CODE,          16#00).
-define(IPROTO_SYNC,          16#01).


-define(REQUEST_CODE_SELECT,  16#01).
-define(REQUEST_CODE_INSERT,  16#02).
-define(REQUEST_CODE_REPLACE, 16#03).
-define(REQUEST_CODE_UPDATE,  16#04).
-define(REQUEST_CODE_DELETE,  16#05).
-define(REQUEST_CODE_CALL,    16#06).
-define(REQUEST_CODE_EVAL,    16#08).
-define(REQUEST_CODE_UPSERT,  16#09).


%% Body Keys
-define(SPACE_ID,             16#10).
-define(INDEX_ID,             16#11).
-define(LIMIT,                16#12).
-define(OFFSET,               16#13).
-define(ITERATOR,             16#14).
-define(KEY,                  16#20).
-define(TUPLE,                16#21).
-define(FUNC_NAME,            16#22).
-define(EXPRESSION,           16#27).
-define(OPS,                  16#28).



%-type tree() :: {'node', Left::tree(), Right::tree(), Key::any(), Value::any()}.
-type tarantool_db_conn() :: {ok, atom()|list()} |
                             atom() |
                             list().

-type tarantool_return() :: ok |
                            {ok, term()} |
                            {err, {integer(), binary()}} |
                            {err, term()}.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Connect
connect() ->
  connect("unnamed").

connect(Name) -> 
  connect(Name, []).

% See taran_socket_holder:init_ for connect Options
-spec connect(Name::atom()|list(), Options::list()) -> 
    {ok, DBConn::term()} | {err, Reason::term()}.
connect(Name, Options) when is_atom(Name); Name == "unnamed" -> 

  SupConnName =
    case Name == "unnamed" of
      true  -> Name ++ "_" ++ random_str(8);
      false -> Name
    end,

  {M, F, A} = {taran_conns_sup, start_link, [Name, Options]},
  ChildSpec = {SupConnName, {M, F, A}, temporary, 5000, supervisor, [M]},

  case supervisor:start_child(taran_sup, ChildSpec) of
    {ok, _} -> {ok, SupConnName};
    Else    -> Else
  end.
  

-spec connect_list() -> SupChailds::list().
connect_list() -> 
  supervisor:which_children(taran_sup).


-spec connect_close(ConnId::atom()|list()|{ok, ConnId::atom()|list()}) -> 
    TerminateChildReturn::term().
connect_close({ok, ConnId}) ->
  connect_close(ConnId);
connect_close(ConnId) -> 
  supervisor:terminate_child(taran_sup, ConnId).


-spec connect_close_all() -> RetList::list().
connect_close_all() ->
  [connect_close(ConnId) || {ConnId, _,_,_} <- connect_list()].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Get socket handler worker pid 
%% @local 
get_conn_pid({ok, Conn}) ->
  get_conn_pid(Conn);
get_conn_pid(Conn) when is_list(Conn); is_atom(Conn) ->
  case lists:keysearch(Conn, 1, supervisor:which_children(taran_sup)) of
    {value, {_,SupPid,_,_}} ->
      case [WrkPid || {_,WrkPid,_,_} <- supervisor:which_children(SupPid), is_pid(WrkPid)] of
        WrkPids when WrkPids /= [] -> 
          {ok, lists:nth(random_int(length(WrkPids)), WrkPids)};
        _ -> 
          {err, empty_sockets}
      end;
    false -> {err, no_such_connection}
  end;
get_conn_pid(Conn) -> 
  {err, Conn}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Send to request
%% @local
send(Conn, Code, Body) ->
  Req = #{
    code => Code,
    body => Body},

  case get_conn_pid(Conn) of
    {ok, Pid} -> taran_socket_holder:req(Pid, Req, 1*1000);
    Else -> Else
  end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% SELECT
%% Args = proplist with [space_id, index_id, limit, offset, iterator] keys
%% example:
%%  select(test_conn, [1], [{limit, 5}]).
select(Conn) -> 
  select(Conn, []).
select(Conn, Key) ->
  select(Conn, Key, []).

-spec select(Conn::tarantool_db_conn(), Key::list(), Args::list()) -> 
    TarantoolReturn::tarantool_return().
select(Conn, Key, Args) ->
  SpaceId   = proplists:get_value(space_id, Args, 16#00),       %% 0 by default
  Index     = proplists:get_value(index_id, Args, 16#00),       %% 0 (primary?) by default
  Limit     = proplists:get_value(limit,    Args, 16#FFFFFFFF), %% Very big by default
  OffSet    = proplists:get_value(offset,   Args, 16#00),       %% No offset by default
  Iterator  = proplists:get_value(iterator, Args, 16#00),       %% EQ by default

  Body = msgpack:pack(#{
    ?SPACE_ID => SpaceId,
    ?INDEX_ID => Index,
    ?LIMIT    => Limit,
    ?OFFSET   => OffSet,
    ?ITERATOR => Iterator,
    ?KEY      => Key}),

  send(Conn, ?REQUEST_CODE_SELECT, Body).  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% INSERT/REPLACE
insert(Conn, Tuple) ->
  insert(Conn, Tuple, []).

-spec insert(Conn::tarantool_db_conn(), Tuple::list(), Args::list()) -> 
    TarantoolReturn::tarantool_return().
insert(Conn, Tuple, Args)  when is_list(Tuple) ->
  SpaceId   = proplists:get_value(space_id, Args, 16#00),       %% 0 by default
  
  Body = msgpack:pack(#{
    ?SPACE_ID => SpaceId,
    ?TUPLE    => Tuple}),

  send(Conn, ?REQUEST_CODE_INSERT, Body).

%
replace(Conn, Tuple) ->
  replace(Conn, Tuple, []).

-spec replace(Conn::tarantool_db_conn(), Tuple::list(), Args::list()) ->
    TarantoolReturn::tarantool_return().
replace(Conn, Tuple, Args) ->
  SpaceId   = proplists:get_value(space_id, Args, 16#00),       %% 0 by default
  ListTuple = tuple_to_list(Tuple),

  Body = msgpack:pack(#{
    ?SPACE_ID => SpaceId,
    ?TUPLE    => ListTuple}),

  send(Conn, ?REQUEST_CODE_REPLACE, Body).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% UPDATE
update(Conn, Key, Op) ->
  update(Conn, Key, Op, []).

-spec update(Conn::tarantool_db_conn(), Key::list(), Op::list(), Args::list()) ->
    TarantoolReturn::tarantool_return().
update(Conn, Key, Op, Args) ->
  SpaceId   = proplists:get_value(space_id, Args, 16#00),       %% 0 by default
  Index     = proplists:get_value(index_id, Args, 16#00),       %% 0 (primary?) by default
  % {$+, 2, 1}
  
  Body = msgpack:pack(#{
    ?SPACE_ID => SpaceId,
    ?INDEX_ID => Index,
    ?KEY      => Key,
    ?TUPLE    => Op}),

  send(Conn, ?REQUEST_CODE_UPDATE, Body).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DELETE
delete(Conn, Key) ->
  delete(Conn, Key, []).

-spec delete(Conn::tarantool_db_conn(), Key::list(), Args::list()) ->
    TarantoolReturn::tarantool_return().
delete(Conn, Key, Args) ->
  SpaceId   = proplists:get_value(space_id, Args, 16#00),         %% 0 by default
  Index     = proplists:get_value(index_id, Args, 16#00),       %% 0 (primary?) by default

  Body = msgpack:pack(#{
    ?SPACE_ID => SpaceId,
    ?INDEX_ID => Index,
    ?KEY      => Key}),

  send(Conn, ?REQUEST_CODE_DELETE, Body).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% CALL
call(Conn) ->
  call(Conn, <<"tonumber">>, [<<"5">>]).
call(Conn, FuncName) ->
  call(Conn, FuncName, []).


-spec call(Conn::tarantool_db_conn(), FuncName::binary(), FuncArgs::list()) ->
    TarantoolReturn::tarantool_return().
call(Conn, FuncName, FuncArgs) ->
  
  <<Body/binary>> = msgpack:pack(#{
    ?FUNC_NAME => FuncName,
    ?TUPLE     => FuncArgs}),

  send(Conn, ?REQUEST_CODE_CALL, Body).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% EVAL
eval(Conn) ->
  eval(Conn, <<"return 'Hello world!'">>).
eval(Conn, Expr) ->
  eval(Conn, Expr, []).

-spec eval(Conn::tarantool_db_conn(), Expr::binary(), ArgsList::list()) ->
    TarantoolReturn::tarantool_return().
eval(Conn, Expr, ArgsList) ->
  Body = msgpack:pack(#{
    ?EXPRESSION => Expr,
    ?TUPLE      => ArgsList}),

  send(Conn, ?REQUEST_CODE_EVAL, Body).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% UPSERT
upsert(Conn, Tuple, Ops) ->
  upsert(Conn, Tuple, Ops, []).

-spec upsert(Conn::tarantool_db_conn(), Tuple::list(), Ops::list(), Args::list()) ->
    TarantoolReturn::tarantool_return().
upsert(Conn, Tuple, Ops, Args) -> 
  SpaceId   = proplists:get_value(space_id, Args, 16#00),       %% 0 by default
  ListTuple = tuple_to_list(Tuple),

  Body = msgpack:pack(#{
    ?SPACE_ID => SpaceId,
    ?TUPLE    => ListTuple,
    ?OPS      => Ops}),

  send(Conn, ?REQUEST_CODE_UPSERT, Body).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%





% MISC
random_str(Length) ->
  AllowedChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789",
  {A,B,C} = os:timestamp(),
  random:seed(A,B,C),
  lists:foldl(
    fun(_, Acc) ->
      [lists:nth(random:uniform(length(AllowedChars)), AllowedChars)] ++ Acc
    end, [], lists:seq(1, Length)).

random_int(1) -> 1;
random_int(N) ->
  {A,B,C} = erlang:timestamp(),
  random:seed(A,B,C),
  random:uniform(N).

