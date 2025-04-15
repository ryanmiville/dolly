-module(dolly_test_ffi).

-export([receive_eventually/3]).

receive_eventually({subject, _Pid, Ref}, Message, Timeout) ->
    receive
        {Ref, Message} ->
            {ok, Message}
    after Timeout ->
        {error, nil}
    end.
