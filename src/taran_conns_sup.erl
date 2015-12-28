%%
%% Moduele to start and manage connections to tarantool server
%%
-module(taran_conns_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================
start_link("unnamed", Args) -> 
    supervisor:start_link(?MODULE, Args);
start_link(Name, Args) -> 
    supervisor:start_link({local, Name}, ?MODULE, Args).
                

%% ============ =======================================================
%% Supervisor callbacks
%% ===================================================================

init(Args) ->
    {ConnsNum, NewArgs} =
      case lists:keytake(conns_num, 1, Args) of
        {value, {conns_num, Value}, NewArgsValue} -> {Value, NewArgsValue};
        _ -> {3, Args}
      end,

    F = 
      fun(Name, Num) -> 
        list_to_atom(atom_to_list(Name) ++ "_" ++ integer_to_list(Num))
      end,
    
    Childs = [#{id      => F(taran_socket_holder, N),
                start   => {taran_socket_holder, start_link, [NewArgs]},
                type    => worker,
                modules => [taran_socket_holder]} || N <- lists:seq(1, ConnsNum)],
    %io:format("~p~n", [Childs]),
    {ok, { {one_for_one, 3, 10}, Childs} }.


