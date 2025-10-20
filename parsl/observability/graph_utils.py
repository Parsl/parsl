from parsl.observability.utils import group_by_keys


def plot_task_lines(logs, plt):
    # these next two dict iterations are fused together
    # to avoid intermediate tgroups2 dict. I'm not sure about the
    # hackability there: is selection of events different from
    # turning those events into a tuple? Probably not because
    # they're both dealing with the same ad-hoc in-query schema.

    tgroups = group_by_keys(logs, ('parsl_dfk', 'parsl_task_id',))

    tasks = []
    for t, v in tgroups.items():
        tlogs = [lr
                 for lr in v
                 if 'parsl_task_status' in lr and
                    (lr['parsl_task_status'] == 'running' or
                     lr['parsl_task_status'] == 'running_ended')]
        assert len(tlogs) == 2, "should have start and end events"

        # now turn each group, of two lines, into a start and
        # end event pair.

        # for k, tg in tgroups2.items():
        k = t
        tg = tlogs
        print(k, tg)

        start_records = [lr
                         for lr in tg
                         if lr['parsl_task_status'] == 'running']

        assert len(start_records) == 1
        s = start_records[0]['created']  # TODO: get_assert_unique helper

        end_records = [lr
                       for lr in tg
                       if lr['parsl_task_status'] == 'running_ended']

        assert len(end_records) == 1
        e = end_records[0]['created']  # TODO: get_assert_unique helper - that could feed the whole collection into?

        app = end_records[0]['parsl_app_name']  # TODO: or for more strictness, the assert_unique_key_key('parsl_app_name')

        task_id = start_records[0]['parsl_task_id']

        # TODO: this needs to be more automatic, or selectable
        # rather than hard-coded task names.
        if app == 'a':
            c = 'red'
        elif app == 'b_left':
            c = 'cyan'
        elif app == 'b_right':
            c = 'blue'
        else:
            c = 'green'

        tasks.append((int(task_id), s, e, app, c))

    # what did this sort do? plotting should be commutative?
    # tasks.sort(key=lambda t: t[0])

    # print(tasks)

    # theres some weirdness that depending on the xlim (explicit or implied) either many
    # plots are not getting plotted or they y-axis is getting rounded or maybe they are
    # so short on the big scale that they don't show up?
    # I think its that last one - maybe I should add end points not just line (eg maybe
    # there is no line filling... and what i'm seeing is some GC-pulsing carried into
    # rounding (!))
    # the only change is changing plt.xlim, not any of my data generation/plot calls.

    # -14000, -12000 - denser y-axis(!) like its slowly converging to correct resolution?
    # -15000, -10000 bad
    # -15000, -1000 bad
    # plt.xlim(-15000, -10000)
    plt.xlabel("walltime/s")
    plt.ylabel("task ID")
    labelled = set()
    for t in tasks:
        # constant colouring for showing flow of execution
        # c = 'blue'

        pl = plt.plot([t[1], t[2]], [t[0], t[0]], '-', marker='.', color=t[4])

        # label each app name once, even though apps will have many
        # invocations (= plt.plot invocations)
        if t[3] not in labelled:
            pl[0].set_label(t[3])
            labelled.add(t[3])

    plt.legend()
