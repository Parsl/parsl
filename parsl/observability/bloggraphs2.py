import parsl.observability.rerun_monitoring_db as mdb

from parsl.observability.utils import get_all_values, group_by_keys, widen_by_implication

from parsl.observability.graph_utils import plot_task_lines

WW = 45

if __name__ == "__main__":

  # all_logs will keep all the logs we've read, while
  # logs will be used to focus in on specific records.

  all_logs = mdb.rerun("runinfo/monitoring.db")

  logs = all_logs

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
  # plotname3 = 'run147_c_completion.png'

  # run 150 - priorities 3,2,1 on A, Bs, C
  # fa117c4e-f300-47f7-a9c5-023b72337a44
  # chosen_run_id = 'fa117c4e-f300-47f7-a9c5-023b72337a44'
  # plotname = 'run150_total.png'
  # plotname2 = 'run150_tasks.png'
  # plotname3 = 'run150_c_completion.png'

  #run 152 - random priorities
  # chosen_run_id = 'ac414cf5-75ca-4aeb-bea9-78b20265731e'
  # plotname = 'run152_total.png'
  # plotname2 = 'run152_tasks.png'
  # plotname3 = 'run152_c_completion.png'

  # run 155 - task cluster priorities
  # chosen_run_id = 'a63732c8-34d0-4012-aabe-6c344853ad5f'
  # plotname = 'run155_total.png'
  # plotname2 = 'run155_tasks.png'
  # plotname3 = 'run155_c_completion.png'

  # run 161 - taskvine no priorities
  # chosen_run_id = '0e2f143e-0840-4b75-a171-99b51b845445'
  # plotname = 'run161_total.png'
  # plotname2 = 'run161_tasks.png'
  # plotname3 = 'run161_c_completion.png'

  chosen_run_id = '0ae352dc-9ddf-4739-9048-b8e23f9b26d9'
  plotname = 'tmp_total.png'
  plotname2 = 'tmp_tasks.png'
  plotname3 = 'tmp_c_completion.png'

  # example of an implication-join here:
  # parsl_dfk, parsl_task_id => parsl_app_name
  # so check and propagate out parsl_app_names.

  # this is widen by implication: parsl_dfk, parsl_task_id => parsl_app_name

  # the task identity implies the name of the app being run in that task
  widen_by_implication(logs, ('parsl_dfk', 'parsl_task_id'), 'parsl_app_name')

  # example of danger: this is UUID comparison using string operations
  logs = [l for l in logs if l['parsl_dfk'] == chosen_run_id]

  print(f"Selected by run id, there are {len(logs)} entries")
  assert len(logs) > 0, "we should have found some logs" 

  # now select only running and running_ended events.

  logs = [l for l in logs
            if 'parsl_task_status' in l
            and (l['parsl_task_status'] == 'running'
                 or l['parsl_task_status'] == 'running_ended')]

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
  # although we pick the start time from logs, we modify *all*
  # log records -- so that if we then go use the logs elsewhere
  # (which I do) then they aren't using two bases.
  start_time = min(*[l['created'] for l in logs])

  # this is a rewrite_by_lambda
  for l in all_logs:
      if 'created' in l:
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
  plt.xlim(0, WW)
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

  logs = all_logs

  # this will be selectable by UI in the webui version
  chosen_run_id = '0ae352dc-9ddf-4739-9048-b8e23f9b26d9'

  # logs = [l for l in logs if l['parsl_dfk'] == chosen_run_id]

  assert len(logs) > 0, "we should have found some logs" 

  dpi = 100
  plt.figure(figsize=(800 / dpi, 800 / dpi))

  plot_task_lines(logs, plt)

  plt.savefig(plotname2, dpi=dpi)
  plt.close()

  # now I have the app name I can plot completion rates for only the C app
  # ignoring the other three - that is, to plot individual diamond completions.

  # for refactoring:
  # this is like counting across the graph, like in the first plot
  # but the counts are +1 for every c app thats end, and never a -1

  tsv = [(l['created'], 1 if l['parsl_task_status'] == 'running_ended' and l['parsl_app_name'] == 'c' else 0) for l in logs if 'created' in l and 'parsl_task_status' in l and 'parsl_app_name' in l]

  tsv.sort(key=lambda l: float(l[0]))

  # fold to sum

  tsv2 = []
  running_total = 0
  for (timestamp, delta) in tsv:
      running_total += delta
      tsv2.append( (timestamp, running_total) )

  assert len(tsv2) == len(tsv), "these are rewrites not filters"
  # assert len(tsv2) == len(logs), "these are rewrites not filters"

  dpi = 100
  plt.figure(figsize=(800 / dpi, 400 / dpi))
  plt.xlim(0, WW)
  plt.xlabel("walltime/s")
  plt.ylabel("C tasks completed")
  plt.plot([t[0] for t in tsv2], [t[1] for t in tsv2])

  plt.savefig(plotname3, dpi=dpi)
  plt.close()

