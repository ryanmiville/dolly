-module(dolly_test_ffi).

-export([should_receive/3, should_not_receive/3]).

-include_lib("eunit/include/eunit.hrl").

should_receive({subject, _Pid, Ref}, Message, Timeout) ->
    receive
        {Ref, Message} ->
            nil
    after Timeout ->
        ?assertEqual(Message, timeout),
        nil
    end.

should_not_receive({subject, _Pid, Ref}, Message, Timeout) ->
    receive
        {Ref, Message} ->
            ?assertEqual(nil, Message),
            nil
    after Timeout ->
        nil
    end.
