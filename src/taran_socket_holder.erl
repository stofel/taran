-module(taran_socket_holder).
-behaviour(gen_server).

-export([start_link/1, handle_info/2, code_change/3, terminate/2]).
-export([init/1, handle_call/3, handle_cast/2, code_reload/0]).

-export([req/2, req/3]).


-define(IPROTO_CODE,          16#00).
-define(REQUEST_CODE_SELECT,  16#01).

-define(IPROTO_SYNC,          16#01).
-define(REQUEST_SYNC_TRUE,    16#01).
-define(REQUEST_SYNC_FALSE,   16#00).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% OTP gen_server api
start_link(Args) -> gen_server:start_link(?MODULE, Args, []).

init(Args) -> init_(Args).
handle_info(Message, State) -> info_(State, Message).
code_reload() -> code_change([], #{}, []).
code_change(_OldVersion, State, _Extra) -> {ok, State}.
terminate(_Reason, _State) -> ok.

%%casts
handle_cast({run, _FunName, Fun, Args}, State) -> apply(Fun, [State|Args]);
handle_cast(_Req, State) -> {noreply, State}.
%%calls
handle_call({run, _FunName, Fun, Args}, From, State) -> apply(Fun, [State,From|Args]);
handle_call(_Req, _From, State) -> {reply, unknown_command, State}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


init_(Args) ->
  Address = proplists:get_value(host, Args, "localhost"),
  Port    = proplists:get_value(port, Args, 3301),
  TcpOpts = [
        {exit_on_close, true},
        {mode, binary},
        {packet, raw},
%        {delay_send, Mode =/= blocked},
        {keepalive, true}],
  {ok, Socket} = gen_tcp:connect(Address, Port, TcpOpts),
  Greeting = receive StartResponse -> StartResponse after 1000 -> timeout end,

  Res =
    case Greeting of
      timeout -> {err, {timeout, server_connect_timeout}};
      {tcp, Socket, GreetingData} -> 
        %%
        case {proplists:get_value(user, Args, <<"none">>),
              proplists:get_value(pass, Args, <<"none">>)} of
          {<<"none">>, <<"none">>} ->
            {ok, #{s => Socket, refs => [], buf => <<>>, seq => 1}};
          {Login, Password} ->
            case auth(Socket, Login, Password, GreetingData) of
              ok -> {ok, #{s => Socket, refs => [], buf => <<>>, seq => 1}};
              Else -> Else
            end
        end;
      _ -> {err, {server_connect_error, server_connect_error}}
    end,
  %io:format("~p~n", [{Res, Greeting}]),
  Res.
 


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% AUTH
%% construct and send auth request and receive and parse response.
-define(IPROTO_LOGIN,  16#23).
-define(IPROTO_CRED,   16#21).
auth(Socket, Login, Password, Greeting) when is_binary(Login), is_binary(Password) ->

  Header = msgpack:pack(#{
    ?IPROTO_CODE => 16#07,
    ?IPROTO_SYNC => 16#00
  }),

  Scrample = scramble(Greeting, Password),

  Body = msgpack:pack(#{
    ?IPROTO_LOGIN => Login,
    ?IPROTO_CRED  => [<<"chap-sha1">>, Scrample]
  }),

  Len = msgpack:pack(erlang:size(Body) + erlang:size(Header)),
  Packet = <<Len/binary, Header/binary, Body/binary>>,
  gen_tcp:send(Socket, Packet),
  AuthRes =
    receive 
      {tcp, Socket, AuthResponse} -> 
        UnpackFun =
          fun
            (Bin, AnswerAcc, F) when Bin /= <<>> ->
              case msgpack:unpack_stream(Bin) of
                {Term,  RestBin} when Term /= error -> F(RestBin, [Term|AnswerAcc], F);
                {error, Reason} -> throw(Reason)
              end;
            (<<>>, AnswerAcc, _F) -> lists:reverse(AnswerAcc)
          end,
        case UnpackFun(AuthResponse, [], UnpackFun) of 
          [_,#{0 := 0,1 := 0},#{}]    -> ok;
          [_,#{0 := _,1 := _}, Else]  -> {err, Else};
          _ -> {err, auth_err}
        end
    after 
      1000 -> {err, no_auth_response}
    end,
  %io:format("AuthRes: ~p~n", [AuthRes]),
  AuthRes;

auth(Socket, Login, Password, Greeting) when is_list(Login) ->
  auth(Socket, list_to_binary(Login), Password, Greeting);
auth(Socket, Login, Password, Greeting) when is_list(Password) ->
  auth(Socket, Login, list_to_binary(Password), Greeting).


scramble(<<_Greeting:64/binary, GreetingSalt:44/binary, _Rest/binary>>, Password) ->
  % http://tarantool.org/doc/dev_guide/box-protocol.html#iproto-protocol
  Salt      = binary:part(base64:decode(GreetingSalt), 0, 20),
  Hash1     = crypto:hash(sha, Password),
  Hash2     = crypto:hash(sha, Hash1),
  
  Context1  = crypto:hash_init(sha),
  Context2  = crypto:hash_update(Context1, Salt),
  Context3  = crypto:hash_update(Context2, Hash2),
  Scramble1 = crypto:hash_final(Context3), 
  
  Scramble  = crypto:exor(Hash1, Scramble1),

  Scramble. 
  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% REQUEST
req(Pid, Req) -> 
  req(Pid, Req, 5000).
req(Pid, Req, Timeout) -> 
  Ref = erlang:make_ref(),
  gen_server:cast(Pid, {run, req_, fun req_/5, [Ref, Req, self(), Timeout]}),
  receive
    {taran_socket_answer, Ref, Answer} -> Answer
  after
    Timeout+200 -> {err, {timeout, request_timeout}}
  end.
req_(State = #{s := Socket, refs := Refs, seq := Seq}, Ref, Req, Pid, Timeout) ->
  #{code := Code, body := Body} = Req,
  HeaderMap = #{
    ?IPROTO_CODE    => Code, %% Select
    ?IPROTO_SYNC    => Seq + 1},
  Header = msgpack:pack(HeaderMap),

  ReqLen = msgpack:pack(erlang:size(Header) + erlang:size(Body)),

  Packet = <<ReqLen/binary, Header/binary, Body/binary>>,

  ok = gen_tcp:send(Socket, Packet),
  erlang:send_after(Timeout, self(), {req_timeout, Ref}),

  {noreply, State#{refs := [{Ref, Seq+1, Pid}|Refs]}}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% process info messages, timeouts of soket signals and data received 
info_(State, {tcp,         Socket, Response}) -> recv_(State,  {tcp, Socket, Response});
info_(State, {tcp_closed,  Socket})           -> close_(State, Socket, socket_closed);
info_(State, {tcp_error,   Socket, Reason})   -> close_(State, Socket, Reason);
info_(State, {req_timeout, Ref})              -> req_timeout_(State, Ref);
info_(State, timeout)                         -> timeout_(State);
info_(State, _)                               -> info_unknown(State).


req_timeout_(State = #{refs := Refs}, Ref) ->
  {noreply, State#{refs := lists:keydelete(Ref, 1, Refs)}}.
  

timeout_(State) -> {noreply, State}.
info_unknown(State) ->
  % print msg
  {noreply, State}.


close_(_State, _Socket, Reason) ->
  %exit(err),
  throw({tcp_closed, Reason}).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% RECEIVE & UNPACK & ANSWER
recv_(State = #{buf := Buf, s := Socket}, {tcp, Socket, Packet}) ->
  {NewBuf, Answers} = parse_(<<Buf/binary, Packet/binary>>),
  NewState = resp_(State, Answers),
  {noreply, NewState#{buf := NewBuf}}.


  

parse_(BufPacket) -> 
  {NewBuf, AnswerBins} = parse_(BufPacket, _AnswerAcc = []),
  UnpackFun = 
    fun
      (Bin, AnswerAcc, F) when Bin /= <<>> ->
        case msgpack:unpack_stream(Bin) of
          {Term,  RestBin} when Term /= error -> F(RestBin, [Term|AnswerAcc], F);
          {error, Reason} -> throw(Reason)
        end;
      (<<>>, AnswerAcc, _F) -> lists:reverse(AnswerAcc)
    end,
  Answers = [UnpackFun(AnswerBin, [], UnpackFun) || AnswerBin <- AnswerBins],
  {NewBuf, Answers}. 
  

parse_(BufPacket, AnswerAcc) when BufPacket /= <<>> -> 
  case msgpack:unpack_stream(BufPacket) of
    {Len, RestBin} when is_integer(Len) ->
      case RestBin of
        <<RestBin:Len/binary>> -> parse_(<<>>, [RestBin|AnswerAcc]);
        <<RestBin:Len/binary, NewBuf>> -> parse_(NewBuf, [RestBin|AnswerAcc]);
        IncompleteRest ->
          case msgpack:unpack_stream(IncompleteRest) of
            {#{?IPROTO_CODE := Code, ?IPROTO_SYNC := Sync}, _Rest} when 
                is_integer(Code), is_integer(Sync) ->
                  %% So yes, it is like to begin of next packet
                  {BufPacket, AnswerAcc};
            _ ->
                  %% No it is like ugly unknown crap
                  throw(crap_received)
          end
      end;
    {error, Reason} -> throw({crap_received2, Reason})
  end;
parse_(<<>>, AnswerAcc) -> {<<>>, AnswerAcc}.


% 
resp_(State = #{refs := Refs}, [[Header = #{0 := 0}, #{16#30 := AnswerBody}]|Answers]) ->
  #{?IPROTO_SYNC  := Seq} = Header,
  case lists:keytake(Seq, 2, Refs) of
    {value, {Ref, Seq, Pid}, NewRefs} ->
      Pid ! {taran_socket_answer, Ref, {ok, AnswerBody}},
      resp_(State#{refs := NewRefs}, Answers);
    false ->
      %% dalaed answer? drop it, do_nothing
      resp_(State, Answers)
  end;
%
resp_(State = #{refs := Refs}, [[Header = #{0 := 0}, _]|Answers]) ->
  #{?IPROTO_SYNC  := Seq} = Header,
  case lists:keytake(Seq, 2, Refs) of
    {value, {Ref, Seq, Pid}, NewRefs} ->
      Pid ! {taran_socket_answer, Ref, {ok, []}},
      resp_(State#{refs := NewRefs}, Answers);
    false ->
      %% dalaed answer? drop it, do_nothing
      resp_(State, Answers)
  end;
%
resp_(State = #{refs := Refs}, [[Header = #{0 := ErrCode}, #{16#31 := AnswerBody}]|Answers]) ->
  #{?IPROTO_SYNC  := Seq} = Header,
  case lists:keytake(Seq, 2, Refs) of
    {value, {Ref, Seq, Pid}, NewRefs} ->
       Pid ! {taran_socket_answer, Ref, {err, {ErrCode, AnswerBody}} },
       resp_(State#{refs := NewRefs}, Answers);
    false ->
      %% dalaed answer? drop it, do_nothing
      resp_(State, Answers)
  end;
%
resp_(State, []) -> State.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


