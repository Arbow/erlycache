%%%-------------------------------------------------------------------
%%% erlycache_server: An erlang cache server implement
%%% @author arbow <avindev@gmail.com>
%%% @version 0.1
%%%
%%% The Apache License
%%%
%%% Copyright Arbow 2008
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%-------------------------------------------------------------------
-module(erlycache_server).

-behaviour(gen_server).

%% API
-export([start_link/0, shutdown/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

%% internal funtions export
-export([datum_loop/1, datum_init/1]).

%% test functions export
-export([readline/1, split_space/1]).

%% Server listening port
-define(LISTEN_PORT, 11212).

%% Server cache table name
-define(CACHE_TABLE, cache_table).

%% Response Define
-define(VERSION, <<"VERSION erlycache v0.1\r\n">>).
-define(ERROR_RESPONSE, <<"ERROR\r\n">>).
-define(STORED_RESPONSE, <<"STORED\r\n">>).
-define(NOTSTORE_RESPONSE, <<"NOT_STORED\r\n">>).
-define(GET_END_RESPONSE, "END\r\n").

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the erlycached server
%%--------------------------------------------------------------------
start_link() ->
    %fprof:trace(start),
    io:format("erlycache_server:start_link() invoked~n"),
    gen_server:start_link({local, erlycache}, ?MODULE, [], []).
    
shutdown() ->
    io:format("shutdown erlang cache server~n"),
    gen_server:cast(erlycache, shutdown).

%%====================================================================
%% erlycached server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init([]) -> {ok, DatumIndex}
%% Description: Initiates the erlycached server
%%--------------------------------------------------------------------
init([]) ->
    io:format("erlycache_server:init() invoked~n"),
    process_flag(trap_exit, true),
    %% TODO: if tcp server start failed, exception handle
    case tcp_server:start_raw_server(?LISTEN_PORT, fun(Socket) -> socket_handler(Socket, self()) end,100,0) of
        {ok, TcpServerPid} ->
            DatumIndex = ets:new(?CACHE_TABLE, [set, private]),
            io:format("Starting erlang cache server~n"),
            {ok, DatumIndex};
        {error, Reason} ->
            {stop,Reason}
    end.

%%--------------------------------------------------------------------
%% Function: handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    {reply, 'InvalidRequest', State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cache service requests
%%--------------------------------------------------------------------
handle_cast({add, Key, Value, ExpTime, Flag, Size, From}, DatumIndex) ->
    case find_datum(Key, DatumIndex) of
        {found, _Pid} ->
            From ! {error, key_existed};
        {failed, notfound} ->
            create_datum({From, Key, Value, ExpTime, Flag, Size}, DatumIndex)
    end,
    {noreply, DatumIndex};
handle_cast({set, Key, Value, ExpTime, Flag, Size, From}, DatumIndex) ->
    case find_datum(Key, DatumIndex) of
        {found, Pid} ->
            Pid ! {new_data, {From, Key, Value, ExpTime, Flag, Size}};
        {failed, notfound} ->
            create_datum({From, Key, Value, ExpTime, Flag, Size}, DatumIndex)
    end,
    {noreply, DatumIndex};
handle_cast({get, Key, From}, DatumIndex) ->
    case find_datum(Key, DatumIndex) of
        {found, Pid} ->
            %io:format("Async get key ~p from ~p~n", [Key, Pid]),
            Pid ! {get, From};
        {failed, notfound} ->
            From ! {failed, not_found}
    end,
    {noreply, DatumIndex};
handle_cast(shutdown, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling datum process down event messages
%%--------------------------------------------------------------------
handle_info({'DOWN', _Ref, process, Pid, _Reason}, DatumIndex) ->
    ets:match_delete(DatumIndex, {'_', Pid}),
    {noreply, DatumIndex};
handle_info(Info, State) ->
    io:format("erlycache_server:handle_info(~p,~p) invoked~n", [Info, State]),
    case Info of 
	{'EXIT', State, killed} ->
	    exit(error);
	_Any ->
	    {noreply, State}
    end.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, DatumIndex) ->
    io:format("Stopping erlycached server~n"),
    %TODO: terminate all datum process
    ets:delete(DatumIndex),
    tcp_server:stop(?LISTEN_PORT),
    %fprof:trace(stop),
    ok.

%%%-------------------------------------------------------------------
%%% Server helper functions
%%%-------------------------------------------------------------------

create_datum({From, Key, Value, ExpTime, Flag, Size}, DatumIndex) ->
    Pid = datum_start({From, Key, Value, ExpTime, Flag, Size}),
    erlang:monitor(process, Pid),
    ets:insert(DatumIndex, {Key, Pid}),
    Pid.
    
find_datum(DatumId, DatumIndex) ->
    Location = ets:lookup(DatumIndex, DatumId),
    find_datum(DatumId, DatumIndex, Location).

find_datum(DatumId, _DatumIndex, [{DatumId, Pid}]) ->
    {found, Pid};
find_datum(_DatumId, _DatumIndex, []) ->
    {failed, notfound};
find_datum(_DatumId, _DatumIndex, Reason) ->
    {failed, Reason}.



%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%--------------------------------------------
%%% Cache data object functions
%%%--------------------------------------------

-record(datum_state, {id, data, ttl, flags, bytes}).

datum_start(DataTuple) ->
    spawn(erlycache_server, datum_init, [DataTuple]).

datum_init({From, Key, Value, ExpTime, Flag, Size}) ->
    From ! {ok, stored},
    datum_loop(#datum_state{id=Key, data=Value, ttl=list_to_integer(ExpTime)*1000, flags=Flag, bytes=Size}).

datum_loop(#datum_state{id=Id, data=Data, ttl=TTL, flags=Flag, bytes=Size} = State) ->
    receive

	%% Cache manager requests data to be replaced...
	{new_data, {From, Key, NewValue, NewExpTime, NewFlag, NewSize}} ->
	    From ! {ok, updated},
	    datum_loop(State#datum_state{data=NewValue, ttl=list_to_integer(NewExpTime)*1000, flags=NewFlag, bytes=NewSize});
            
	%% Request for cached data...
	{get, From} when is_pid(From) ->
            %io:format("Async send result to ~p~n", [From]),
	    From ! {get, Id, Data, Flag, Size},
	    datum_loop(State);

	%% Any other request is ignored, but causes code load.
	_ -> ?MODULE:datum_loop(State)

    %after
	%% Time To Live or TTL frees up cache data object memory (must be integer, 'infinity' or 'timeout').
	%%TTL ->
	    %%TerminateReason = timeout,
	    %%io:format("Cache object ~p owned by ~p freed because of a ~w~n", [Id, self(), TerminateReason])
    end.
    

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
socket_handler(Socket, Controller) ->
    socket_handler(Socket,Controller, []).

socket_handler(Socket, Controller, Buffer) ->
    receive
	{tcp, Socket, Binary} ->
	    Request = Buffer ++ binary_to_list(Binary),
	    socket_handle_request(Socket, Controller, Request);
	{tcp_closed, Socket} ->
	    ok;
	Any ->
            io:format("Receive unexpected message ~p~n", [Any]),
	    socket_handler(Socket, Controller, Buffer)
    end.

socket_handle_request(Socket, Controller, []) ->
    socket_handler(Socket, Controller, []);
socket_handle_request(Socket, Controller, RequestBuffer) ->
    %io:format("Receive request ~w~n", [RequestBuffer]),
    case readline(RequestBuffer) of
        {newline, Request, RequestBufferRemaining} ->
            case "quit\r\n" == Request of
                true ->
                    tcp_server:stop(?LISTEN_PORT);
                false ->
                    case handle_request(Socket, list_to_binary(Request), RequestBufferRemaining) of
                        {ok, RemainingBuffer} ->
                            socket_handle_request(Socket, Controller, RemainingBuffer);
                        {not_complete} ->
                            socket_handler(Socket, Controller, RequestBuffer)
                    end
                    
            end;
	{noline, Buffer} ->
	    socket_handler(Socket, Controller, Buffer)
    end.
    
handle_request(Socket, <<"version">>, RequestBufferRemaining) ->
    gen_tcp:send(Socket, ?VERSION),
    {ok, RequestBufferRemaining};
handle_request(Socket, <<"set ", _Bin/binary>>=Request, RequestBufferRemaining) ->
    handle_request_set(Socket, binary_to_list(Request), RequestBufferRemaining);
handle_request(Socket, <<"get ", Bin/binary>>, RequestBufferRemaining) ->
    handle_request_get(Socket, binary_to_list(Bin), RequestBufferRemaining);
handle_request(Socket, Request, RequestBufferRemaining) ->
    %io:format("handle request ~w~n", [Request]),
    gen_tcp:send(Socket, ?NOTSTORE_RESPONSE),
    {ok, RequestBufferRemaining}. %% TODO

handle_request_set(Socket, Request, RequestBufferRemaining) ->
    ["set", Key, Flags, ExpTime, Bytes] = split_space(Request),
    case readline(RequestBufferRemaining) of 
        {newline, Value, Remaining} ->
            %io:format("Store key ~p~n", [Key]),
            gen_server:cast(erlycache, {set, Key, Value, ExpTime, Flags, Bytes, self()}),
            receive
                _Any ->
                    ok
            end,
            gen_tcp:send(Socket, ?STORED_RESPONSE),
            {ok, Remaining};
        {noline, _Buffer} -> {not_complete}
    end.
    
handle_request_get(Socket, Request, RequestBufferRemaining) ->
    RequestKeys = split_space(Request),
    handle_request_get2(Socket, RequestKeys, RequestBufferRemaining, length(RequestKeys)).
    
handle_request_get2(Socket, [], RequestBufferRemaining, KeySize) ->
    handle_request_get3(Socket, RequestBufferRemaining, [], KeySize);
handle_request_get2(Socket, [H|T], RequestBufferRemaining, KeySize) ->
    %io:format("Async cast request to erlycache genserver~n"),
    gen_server:cast(erlycache, {get, H, self()}),
    handle_request_get2(Socket, T, RequestBufferRemaining, KeySize).
    
handle_request_get3(Socket, RequestBufferRemaining, Response, 0) ->
    Response2 = lists:flatten([Response, ?GET_END_RESPONSE]),
    %io:format("Response get:~w~n", [Response2]),
    gen_tcp:send(Socket, list_to_binary(Response2)),
    {ok, RequestBufferRemaining};
handle_request_get3(Socket, RequestBufferRemaining, Response, KeySize) ->
    receive
        {get, Key, Value, Flag, Size} ->
            %io:format("Receive key ~p, value ~p~n", [Key, Value]),
            ResponseLine = ["VALUE ", Key, " ", Flag, " ", Size, "\r\n", Value, "\r\n"],
            handle_request_get3(Socket, RequestBufferRemaining, [ResponseLine|Response], KeySize-1);
        _Any ->
            handle_request_get3(Socket, RequestBufferRemaining, Response, KeySize-1)
    end.

    
%%------------------------------------------------------------------------------
%% readline/1: read a new line from buffer, each line ends with "\r\n"
%%------------------------------------------------------------------------------
readline([]) ->
    {noline, []};
readline([$\r, $\n|T]) ->
    {newline, [], T};
readline([H|T]) ->
    case readline(T) of
	{newline, Line, OtherLine} ->
	    {newline, [H|Line], OtherLine};
	{noline, Line} ->
	    {noline, [H|Line]}
    end.

%%------------------------------------------------------------------------------
%% split_space/1: get line content by space spliter " "
%%------------------------------------------------------------------------------
split_space(Line) ->
    split_space(Line, [], []).

split_space([], [], Tokens) ->
    lists:reverse(Tokens);
split_space([], Token, Tokens) ->
    lists:reverse([lists:reverse(Token)|Tokens]);
split_space([$ |T], [], Tokens) ->
    split_space(T, [], Tokens);
split_space([$ |T], Token, Tokens) ->
    split_space(T, [], [lists:reverse(Token)|Tokens]);
split_space([H|T], Token, Tokens) ->
    split_space(T, [H|Token], Tokens).

