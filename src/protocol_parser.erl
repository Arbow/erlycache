%% Author: Arbow
%% Created: 2008-6-7
%% Description: TODO: Add description to protocol_parser
-module(protocol_parser).

%%
%% Include files
%%

%%
%% Exported Functions
%%
-export([main/0, protocol_handler_thread1_loop/0]).

-export([extract_str_end_with_tag/3, binary_readline/1, binary_split_space/1]).

%% Response Define
-define(VERSION, <<"VERSION erlycache v0.1\r\n">>).
-define(ERROR_RESPONSE, <<"ERROR\r\n">>).
-define(STORED_RESPONSE, <<"STORED\r\n">>).
-define(NOTSTORE_RESPONSE, <<"NOT_STORED\r\n">>).
-define(GET_END_RESPONSE, "END\r\n").

%%
%% API Functions
%%

main() ->
    Thread1 = spawn(?MODULE, protocol_handler_thread1_loop, []),
    Thread1 ! {recv_stream, <<"version\r\n">>},
    Thread1 ! {recv_stream, <<"set name 0 0 5\r\nArbow\r\n">>},
    Thread1 ! {recv_stream, <<"set age 0 0 2\r\n25\r\n">>},
    Thread1 ! {recv_stream, <<"get name age\r\n">>},
    Thread1 ! {recv_stream, <<"quit\r\n">>},
    Thread1 ! start.


%%
%% Local Functions
%%

protocol_handler_thread1_loop() ->
    receive
	start ->
	    protocol_handler(self())
    end,
    receive_protocol_stream(self()).


%% Normal protocol process, maybe slow!!!
protocol_handler(State) ->
    io:format("start protocol handler\r\n"),
    protocol_handler(State, []).

protocol_handler(State, Buffer) ->
    case receive_protocol_stream(State) of
        {recv_stream, Stream} ->
            handle_request(State, Buffer ++ binary_to_list(Stream));
        {eof_stream} ->
            ok
    end.

%% Handle quit request, and forward others to handle_request/3
handle_request(State, []) ->
    protocol_handler(State, []);
handle_request(State, RequestBuffer) ->
    io:format("Receive request ~w~n", [RequestBuffer]),
    case readline(RequestBuffer) of
        {newline, Request, RequestBufferRemaining} ->
            case "quit\r\n" == Request of
                true ->
                    process_quit(State);
                false ->
                    case handle_request(State, list_to_binary(Request), RequestBufferRemaining) of
                        {ok, RemainingBuffer} ->
                            %% Handler other lines in RemainingBuffer
                            handle_request(State, RemainingBuffer);
                        {not_complete} ->
                            protocol_handler(State, RequestBuffer)
                    end
            end;
        {noline, Buffer} ->
            protocol_handler(State, Buffer)
    end.

%% Handle other requests
handle_request(State, <<"version">>, RequestBufferRemaining) ->
    process_get_version(State, ?VERSION),
    {ok, RequestBufferRemaining};
handle_request(State, <<"set ", _Bin/binary>>=Request, RequestBufferRemaining) ->
    %% Req=list_to_binary(Request), and binary_to_list(Req) to next...
    handle_request_set(State, binary_to_list(Request), RequestBufferRemaining);
handle_request(State, <<"get ", Bin/binary>>, RequestBufferRemaining) ->
    handle_request_get(State, binary_to_list(Bin), RequestBufferRemaining);
handle_request(State, Request, RequestBufferRemaining) ->
    not_store_response(State),
    {ok, RequestBufferRemaining}. %% TODO

%% Handler set request
handle_request_set(State, Request, RequestBufferRemaining) ->
    ["set", Key, Flags, ExpTime, Bytes] = split_space(Request),
    case readline(RequestBufferRemaining) of 
        {newline, Value, Remaining} ->
            async_set(Key, Value, ExpTime, Flags, Bytes),
            store_response(State),
            {ok, Remaining};
        {noline, _Buffer} -> {not_complete}
    end.
    
%% Handler get request
handle_request_get(State, Request, RequestBufferRemaining) ->
    RequestKeys = split_space(Request),
    handle_request_get2(State, RequestKeys, RequestBufferRemaining, length(RequestKeys)).
    
handle_request_get2(State, [], RequestBufferRemaining, KeySize) ->
    handle_request_get3(State, RequestBufferRemaining, [], KeySize);
handle_request_get2(State, [H|T], RequestBufferRemaining, KeySize) ->
    %io:format("Async cast request to erlycache genserver~n"),
    async_get(H),
    handle_request_get2(State, T, RequestBufferRemaining, KeySize).
    
handle_request_get3(State, RequestBufferRemaining, Response, 0) ->
    Response2 = lists:flatten([Response, ?GET_END_RESPONSE]),
    %io:format("Response get:~w~n", [Response2]),
    send_response(State, list_to_binary(Response2)),
    {ok, RequestBufferRemaining};
handle_request_get3(State, RequestBufferRemaining, Response, KeySize) ->
    case receive_async_get_results() of
        {get, Key, Value, Flag, Size} ->
            %io:format("Receive key ~p, value ~p~n", [Key, Value]),
            ResponseLine = ["VALUE ", Key, " ", Flag, " ", Size, "\r\n", Value, "\r\n"],
            handle_request_get3(State, RequestBufferRemaining, [ResponseLine|Response], KeySize-1);
        _Any ->
            handle_request_get3(State, RequestBufferRemaining, Response, KeySize-1)
    end.


%%------------------------------------------------------------------------------
%% New binary protocol handle implement
%%------------------------------------------------------------------------------
%% Normal protocol process, maybe slow!!!
binary_protocol_handler(State) ->
    io:format("start binary protocol handler\r\n"),
    binary_protocol_handler(State, <<>>).

binary_protocol_handler(State, Buffer) ->
    case receive_protocol_stream(State) of
        {recv_stream, Stream} ->
            handle_binary_request(State, <<Buffer/binary, Stream/binary>>);
        {eof_stream} ->
            ok
    end.

handle_binary_request(State, Request) ->
    todo.

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

%%------------------------------------------------------------------------------
%% binary_readline/1: read a new line from binary buffer, each line ends with "\r\n"
%%------------------------------------------------------------------------------
binary_readline(<<>>) ->
    {noline, <<>>};
binary_readline(Stream) ->
    case extract_str_end_with_tag(<<"\r\n">>, 0, Stream) of
	{found, Str, StrLength, NextSeekOffset} ->
	    <<_:NextSeekOffset/bytes, RestStream/binary>> = Stream,
	    {newline, Str, RestStream};
	{not_found} ->
	    {noline, Stream}
    end.

binary_split_space(<<>>) ->
    [];
binary_split_space(BinLine) ->
    binary_split_space(BinLine, [], 0).
binary_split_space(BinLine, Tokens, Offset) ->
    case extract_str_end_with_tag(<<" ">>, Offset, BinLine) of
	{found, Str, StrLength, NextSeekOffset} ->
	    binary_split_space(BinLine, [Str|Tokens], NextSeekOffset);
	{not_found} ->
	    <<_:Offset/bytes, RestWord/binary>> = BinLine,
	    lists:reverse([RestWord|Tokens])
    end.

%%------------------------------------------------------------------------------
%% Extract string end with tag by binary match
%%------------------------------------------------------------------------------
extract_str_end_with_tag(Tag, Offset, Data) when is_binary(Tag) ->
    extract_str_end_with_tag2(Tag, size(Tag), Offset, Offset, Data).

extract_str_end_with_tag2(Tag, TagSize, Offset, BeginOffset, Data) ->
    case Data of
	<<_:Offset/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset, Offset+TagSize, Data);
	<<_:Offset/bytes, _:1/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+1, Offset+1+TagSize, Data);
	<<_:Offset/bytes, _:2/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+2, Offset+2+TagSize, Data);
	<<_:Offset/bytes, _:3/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+3, Offset+3+TagSize, Data);
	<<_:Offset/bytes, _:4/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+4, Offset+4+TagSize, Data);
	<<_:Offset/bytes, _:5/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+5, Offset+5+TagSize, Data);
	<<_:Offset/bytes, _:6/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+6, Offset+61+TagSize, Data);
	<<_:Offset/bytes, _:7/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+7, Offset+7+TagSize, Data);
	<<_:Offset/bytes, _:8/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+8, Offset+8+TagSize, Data);
	<<_:Offset/bytes, _:9/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+9, Offset+9+TagSize, Data);
	<<_:Offset/bytes, _:10/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+10, Offset+10+TagSize, Data);
	<<_:Offset/bytes, _:11/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+11, Offset+11+TagSize, Data);
	<<_:Offset/bytes, _:12/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+12, Offset+12+TagSize, Data);
	<<_:Offset/bytes, _:13/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+13, Offset+13+TagSize, Data);
	<<_:Offset/bytes, _:14/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+14, Offset+14+TagSize, Data);
	<<_:Offset/bytes, _:15/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+15, Offset+15+TagSize, Data);
	<<_:Offset/bytes, _:16/bytes, Tag:TagSize/bytes, _/binary>> -> 
	    extract_str_end_with_tag3(BeginOffset, Offset+16, Offset+16+TagSize, Data);
	<<_:Offset/bytes, _:16/bytes, _/binary>> -> 
	    extract_str_end_with_tag2(Tag, TagSize, Offset+16, BeginOffset, Data);
	_ -> {not_found}
    end.

extract_str_end_with_tag3(OffsetBegin, OffsetEnd, NextSeekOffset, Data) ->
    StrLength = OffsetEnd - OffsetBegin,
    %io:format("extract_str_end_with_tag3(~s,~s,~s,~s)~n", [OffsetBegin, OffsetEnd, NextSeekOffset, Data]),
    <<_:OffsetBegin/bytes, Str:StrLength/bytes, _/binary>> = Data,
    {found, Str, StrLength, NextSeekOffset}.


%%------------------------------------------------------------------------------
%% Protocol event callbacks
%%------------------------------------------------------------------------------

receive_protocol_stream(State) ->
    receive
	Any -> Any
    end.

receive_async_get_results() ->
    {get, "name", "Arbow", "0", "5"}.

process_get_version(_State, _Ver) -> io:format("process_get_version~n"), ok.
process_quit(_State) -> io:format("process_quit~n"), ok.

not_store_response(_State) -> ok.
store_response(_State) -> ok.

async_set(Key, Value, ExpTime, Flags, Bytes) -> io:format("set ~s = ~s~n", [Key, Value]), ok.
async_get(Key) -> io:format("get ~s~n", [Key]), ok.

send_response(State, Response) -> io:format("Send response ~s", [Response]), ok.

