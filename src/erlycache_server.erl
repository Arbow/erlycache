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

%% Server listening port
-define(LISTEN_PORT, 11212).

%% Server cache table name
-define(CACHE_TABLE, cache_table).

%% Response Define
-define(VERSION, <<"VERSION erlycache v0.1\r\n">>).
-define(ERROR_RESPONSE, <<"ERROR\r\n">>).
-define(STORED_RESPONSE, <<"STORED\r\n">>).
-define(NOTSTORE_RESPONSE, <<"NOT_STORED\r\n">>).
-define(GET_END_RESPONSE, <<"END\r\n">>).

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
    datum_loop(#datum_state{id=Key, data=Value, ttl=list_to_integer(binary_to_list(ExpTime))*1000, flags=Flag, bytes=Size}).

datum_loop(#datum_state{id=Id, data=Data, ttl=TTL, flags=Flag, bytes=Size} = State) ->
    receive

	%% Cache manager requests data to be replaced...
	{new_data, {From, Key, NewValue, NewExpTime, NewFlag, NewSize}} ->
	    From ! {ok, updated},
	    datum_loop(State#datum_state{data=NewValue, ttl=list_to_integer(binary_to_list(NewExpTime))*1000, flags=NewFlag, bytes=NewSize});
            
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
%%% Network functions - Using binary match for greater efficiency
%%--------------------------------------------------------------------

socket_handler(Socket, Controller) ->
    socket_handler(Socket,Controller, <<>>).

socket_handler(Socket, Controller, Buffer) ->
    receive
	{tcp, Socket, Binary} ->
	    Request = <<Buffer/binary, Binary/binary>>,
	    handle_tcp_request(Socket, Controller, Request);
	{tcp_closed, Socket} ->
	    ok;
	Any ->
            io:format("Receive unexpected message ~p~n", [Any]),
	    socket_handler(Socket, Controller, Buffer)
    end.

handle_tcp_request(Socket, Controller, <<>>) ->
    socket_handler(Socket, Controller, <<>>); % buffer empty, continue receive

handle_tcp_request(Socket, Controller, <<"quit\r\n", Rest/binary>>) ->
    gen_tcp:close(Socket);

handle_tcp_request(Socket, Controller, <<"version\r\n", Rest/binary>>) ->
    gen_tcp:send(Socket, ?VERSION),
    socket_handler(Socket, Controller, Rest);

handle_tcp_request(Socket, Controller, <<"set ", _/binary>>=Request) ->
    handle_set_request(Socket, Controller, Request);

handle_tcp_request(Socket, Controller, <<"get ", _/binary>>=Request) ->
    handle_get_request(Socket, Controller, Request);

handle_tcp_request(Sockc, Controller, Request) ->
    %% TODO if \r\n found in request, gen_tcp:send(Socket, ?NOTSTORE_RESPONSE)
    socket_handler(Sockc, Controller, Request).


handle_set_request(Socket, Controller, Request) ->
    % request at least two lines
    case lib_text:readline(Request) of
	{newline, FirstLine, RestStream1} ->
	    [<<"set">>, Key, Flags, ExpTime, Bytes] = lib_text:split_space(FirstLine),
	    DataBlockSize = list_to_integer(binary_to_list(Bytes)),
	    case RestStream1 of
		<<DataBlock:DataBlockSize/bytes, "\r\n", RestStream2/binary>> ->
		    gen_server:cast(erlycache, {set, Key, DataBlock, ExpTime, Flags, Bytes, self()}),
		    receive
			_Any -> ok %% TODO Should receive more exact message
		    end,
		    gen_tcp:send(Socket, ?STORED_RESPONSE),
		    socket_handler(Socket, Controller, RestStream2);
		_ ->
		    socket_handler(Socket, Controller, Request)
	    end;
	_ ->
	    socket_handler(Socket, Controller, Request)
    end.

handle_get_request(Socket, Controller, Request) ->
    % request at least one line
    case lib_text:readline(Request) of
	{newline, Line, RestStream} ->
	    [<<"get">> | KeyTails] = lib_text:split_space(Line),
	    Response = handle_get_request2(KeyTails, length(KeyTails)),
	    gen_tcp:send(Socket, Response),
	    socket_handler(Socket, Controller, RestStream);
	_ ->
	    socket_handler(Socket, Controller, Request)
    end.

handle_get_request2([], KeySize) ->
    handle_get_request3(KeySize, <<>>);
handle_get_request2([Key|KeysTail], KeySize) ->
    gen_server:cast(erlycache, {get, Key, self()}),
    handle_get_request2(KeysTail, KeySize).
    

handle_get_request3(0, Response) ->
    FinalResponse = <<Response/binary, ?GET_END_RESPONSE/binary>>,
    FinalResponse;
handle_get_request3(KeySizeRemain, Response) ->
    receive
        {get, Key, Value, Flag, Size} ->
            %io:format("Receive key ~p, value ~p~n", [Key, Value]),
	    NewResponse = <<Response/binary, "VALUE ", Key/binary, " ", Flag/binary, " ", Size/binary, "\r\n", Value/binary, "\r\n">>,
	    handle_get_request3(KeySizeRemain-1, NewResponse);
	_ ->
	    handle_get_request3(KeySizeRemain-1, Response)
    end.
    
