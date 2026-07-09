-module(wolff_dynamic_recovery_tests).

-include("wolff.hrl").
-include_lib("eunit/include/eunit.hrl").

recover_dynamic_topics_from_replayq_test() ->
    BaseDir = tmp_dir(),
    ClientId = <<"client:1">>,
    Group = <<"group:1">>,
    Topic1 = <<"prod_01_datas">>,
    Topic2 = <<"/v1/devices/a/datas">>,
    try
        ok = make_replayq(BaseDir, "group=3a1_prod_01_datas", 0, pending),
        ok = make_replayq(BaseDir, "group=3a1_=2fv1=2fdevices=2fa=2fdatas", 2, pending),
        ok = make_replayq(BaseDir, "other_group_prod_01_datas", 0, pending),
        {ok, St} = wolff_producers:init(
            {ClientId, ?NS_TOPIC(Group, ?DYNAMIC), #{
                group => Group,
                replayq_dir => BaseDir,
                recover_dynamic_topics => true
            }}
        ),
        Status = maps:get(producers_status, St),
        ?assertEqual(
            #{
                Topic1 => {not_initialized, 0, replayq_recovery},
                Topic2 => {not_initialized, 0, replayq_recovery}
            },
            Status
        )
    after
        ok = del_dir_r(BaseDir)
    end.

dynamic_replayq_recovery_ignores_empty_and_consumed_queues_test() ->
    BaseDir = tmp_dir(),
    ClientId = <<"client:1">>,
    Group = <<"group:1">>,
    try
        ok = make_replayq(BaseDir, "group=3a1_empty_topic", 0, empty),
        ok = make_replayq(BaseDir, "group=3a1_consumed_topic", 0, consumed),
        {ok, St} = wolff_producers:init(
            {ClientId, ?NS_TOPIC(Group, ?DYNAMIC), #{
                group => Group,
                replayq_dir => BaseDir,
                recover_dynamic_topics => true
            }}
        ),
        ?assertEqual(#{}, maps:get(producers_status, St))
    after
        ok = del_dir_r(BaseDir)
    end.

dynamic_replayq_recovery_is_opt_in_test() ->
    BaseDir = tmp_dir(),
    ClientId = <<"client:1">>,
    Group = <<"group:1">>,
    try
        ok = make_replayq(BaseDir, "group=3a1_prod_01_datas", 0, pending),
        {ok, St} = wolff_producers:init(
            {ClientId, ?NS_TOPIC(Group, ?DYNAMIC), #{
                group => Group,
                replayq_dir => BaseDir
            }}
        ),
        ?assertEqual(#{}, maps:get(producers_status, St))
    after
        ok = del_dir_r(BaseDir)
    end.

tmp_dir() ->
    Unique = integer_to_list(erlang:unique_integer([positive])),
    filename:join(["/tmp", atom_to_list(?MODULE) ++ "_" ++ Unique]).

make_replayq(BaseDir, Segment, Partition, State) ->
    {ok, _} = application:ensure_all_started(replayq),
    Dir = filename:join([BaseDir, Segment, integer_to_list(Partition)]),
    Q0 = replayq:open(#{
        dir => Dir,
        seg_bytes => 1000000,
        sizer => fun(_) -> 1 end
    }),
    Q =
        case State of
            empty ->
                Q0;
            pending ->
                replayq:append(Q0, [<<"pending">>]);
            consumed ->
                Q1 = replayq:append(Q0, [<<"consumed">>]),
                {Q2, AckRef, [_]} = replayq:pop(Q1, #{count_limit => 1, bytes_limit => 1000000}),
                ok = replayq:ack_sync(Q2, AckRef),
                Q2
        end,
    replayq:close(Q).

del_dir_r(Dir) ->
    case file:list_dir(Dir) of
        {ok, Names} ->
            lists:foreach(
                fun(Name) ->
                    Path = filename:join(Dir, Name),
                    case filelib:is_dir(Path) of
                        true -> ok = del_dir_r(Path);
                        false -> _ = file:delete(Path), ok
                    end
                end,
                Names
            ),
            _ = file:del_dir(Dir),
            ok;
        {error, enoent} ->
            ok
    end.
