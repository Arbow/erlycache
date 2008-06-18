%%%
%%% lib_text: a library use to extracted text from binary data
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

-module(lib_text).

-export([readline/1, split_space/1, extract_str_end_with_tag/3]).

%%------------------------------------------------------------------------------
%% readline/1: read a new line from binary buffer, each line ends with "\r\n"
%% returns {found, Line, LineLength, NextSeekOffset} on success, or
%% returns {noline, BinaryData} when failed
%%   Line = binary()
%%   LineLength = number()
%%   NextSeekOffset = number()
%%------------------------------------------------------------------------------
readline(<<>>) ->
    {noline, <<>>};
readline(Stream) when is_binary(Stream) ->
    case extract_str_end_with_tag(<<"\r\n">>, 0, Stream) of
	{found, Str, StrLength, NextSeekOffset} ->
	    <<_:NextSeekOffset/bytes, RestStream/binary>> = Stream,
	    {newline, Str, RestStream};
	{not_found, _} ->
	    {noline, Stream}
    end.

%%------------------------------------------------------------------------------
%% split_space/1: given a binary like <<"how are you">>, return a list 
%%  [<<"how">>, <<"are">>, <<"you">>]
%%------------------------------------------------------------------------------
split_space(<<>>) ->
    [];
split_space(BinLine) ->
    split_space(BinLine, [], 0).
split_space(BinLine, Tokens, Offset) ->
    case extract_str_end_with_tag(<<" ">>, Offset, BinLine) of
	{found, Str, StrLength, NextSeekOffset} ->
	    split_space(BinLine, [Str|Tokens], NextSeekOffset);
	{not_found, _} ->
	    <<_:Offset/bytes, RestWord/binary>> = BinLine,
	    lists:reverse([RestWord|Tokens])
    end.

%%------------------------------------------------------------------------------
%% extract_str_end_with_tag/3: Extract string end with tag by binary match
%% for example, extract_str_end_with_tag(<<",">>, 0, <<"hello, I am arbow">>)
%% will return {found,<<"hello">>,5,6}
%% 5 is length of <<"hello">>, 6 is offset of <<"hello, I am arbow">>, a space.
%% and then extract_str_end_with_tag(<<",">>, 6, <<"hello, I am arbow">>)
%% will return {not_found,<<" I am arbow">>}
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
	    extract_str_end_with_tag3(BeginOffset, Offset+6, Offset+6+TagSize, Data);
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
	_ -> 
	    <<_:BeginOffset/bytes, RestData/binary>> = Data,
	    {not_found, RestData}
    end.

extract_str_end_with_tag3(OffsetBegin, OffsetEnd, NextSeekOffset, Data) ->
    StrLength = OffsetEnd - OffsetBegin,
    %io:format("extract_str_end_with_tag3(~s,~s,~s,~s)~n", [OffsetBegin, OffsetEnd, NextSeekOffset, Data]),
    <<_:OffsetBegin/bytes, Str:StrLength/bytes, _/binary>> = Data,
    {found, Str, StrLength, NextSeekOffset}.

