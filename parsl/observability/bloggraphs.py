import parsl.observability.rerun_monitoring_db as mdb

if __name__ == "__main__":

  logs = mdb.rerun("runinfo/monitoring.db")

  print(f"rerun of monitoring.db returned {len(logs)} entries")

  print(f"For example: {logs[0]}")

  # select the run I want, by run_id - although actually my notes are by rundir.

  # run #024 - htex no-priority 3b694530-08d8-482d-bad9-2fef260bd223
  # chosen_run_id = '3694530-08d8-482d-bad9-2fef260bd223'

  # run #034 - htex with prioritisation 
  # chosen_run_id = 'c9ea1d09-f836-4a33-ade1-24abeefda943'
  # plotname = 'run034_total.png'
  # plotname2 = 'run034_tasks.png'

  # run 147
  # a7edbee0-c224-4cba-8107-4df24eff8e85
  # chosen_run_id = 'a7edbee0-c224-4cba-8107-4df24eff8e85'
  # plotname = 'run147_total.png'
  # plotname2 = 'run147_tasks.png'

  
  chosen_run_id = '70759e81-fd0f-4f51-9d96-037c197e0852'
  plotname = 'ccpr4279_total.png'
  plotname2 = 'ccpr4279_tasks.png'

  # example of danger: this is UUID comparison using string operations
  logs = [l for l in logs if l['parsl_dfk'] == chosen_run_id]

  print(f"Selected by run id, there are {len(logs)} entries")
  assert len(logs) > 0, "we should have found some logs" 

  # now select only running and running_ended events.

  logs = [l for l in logs
            if 'parsl_task_status' in l and
            (l['parsl_task_status'] == 'running' or l['parsl_task_status'] == 'running_ended')]

  print(f"Selected by status, there are {len(logs)} entries")

  # now two kinds of plots, with two kinds of further data:
  # tasks running by time
  # for that, I don't need task identity. instead I want
  # a list of running (+1) and running_ended (-1) ordered by
  # time, and plot the cumulative sum as a square plot.
  # this can work for any paired events and probably can be
  # factored out: given any selection of records which has a
  # start and stop notion, which is record type specific,
  # or query specific, the plotting code for that is
  # factorable.

  # so write it that way right now:
  # first do the mapping, second do the plotting

  # rewrite with normalised time:
  start_time = min(*[l['created'] for l in logs])
  for l in logs:
      l['created'] -= start_time

  tsv = [(l['created'], 1 if l['parsl_task_status'] == 'running' else -1) for l in logs]


  tsv.sort()  # into timestamp order

  # fold to sum

  tsv2 = []
  running_total = 0
  for (timestamp, delta) in tsv:
      running_total += delta
      tsv2.append( (timestamp, running_total) )

  assert len(tsv2) == len(tsv), "these are rewrites not filters"
  assert len(tsv2) == len(logs), "these are rewrites not filters"

  print(tsv2)


  # now we have the dataset of count transitions.

  # add in additional transitions to make it square

  tsv3 = []
  last_score = 0
  for t in tsv2:
      (timestamp, score) = t
      if last_score is not None:
          tsv3.append( (timestamp, last_score) )
      tsv3.append(t)
      last_score = score

  import matplotlib.pyplot as plt


  dpi = 100
  plt.figure(figsize=(800 / dpi, 400 / dpi))
  plt.xlim(0, 150)
  plt.xlabel("walltime/s")
  plt.plot([t[0] for t in tsv3], [t[1] for t in tsv3])

  plt.savefig(plotname, dpi=dpi)
  plt.close()

  # the second set of plots:
  # one horizontal line per task
  # showing the period task was running
  # period this "span" was in progres.

  # go back to our logs collection which is already start/end
  # events only.
  # now instead of going across by time, cluster them by
  # task ID.
  # and then turn each cluster into a line.

  tgroups = {}  # maybe a 'defaultdict' would be better for this?
  for l in logs:
      task_id = l['parsl_task_id']
      if task_id not in tgroups:
          tgroups[task_id] = []
      tgroups[task_id].append(l)  # group, don't reshape the log entry

  for t, v in tgroups.items():
      assert len(v) == 2, "should have start and end events"

  # now turn each group, of two lines, into a start and
  # end event pair.

  tasks = []
  for k, tg in tgroups.items():
      start_records = [l for l in tg
                         if l['parsl_task_status'] == 'running']
      assert len(start_records) == 1
      s = start_records[0]['created']  # TODO: get_assert_unique helper
  
      end_records = [l for l in tg
                       if l['parsl_task_status'] == 'running_ended']
      assert len(end_records) == 1
      e = end_records[0]['created']  # TODO: get_assert_unique helper - that could feed the whole collection into?

      task_id = start_records[0]['parsl_task_id']

      tasks.append( (task_id, s, e) )


  dpi = 100
  plt.figure(figsize=(800 / dpi, 400 / dpi))
  plt.xlim(0, 150)
  plt.xlabel("walltime/s")
  plt.ylabel("task ID")
  for t in tasks:
      # plot([created, created], [src_y, dest_y], '-', color=c)

      # plt.plot([t[1]], [int(t[0])], "-*", [t[2]], [int(t[0])])
      plt.plot([t[1], t[2]], [int(t[0]), int(t[0])], '-', color='blue')

  plt.savefig(plotname2, dpi=dpi)
  plt.close()



