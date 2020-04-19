#!/usr/bin/env python3
import calendar, json, locale, os, re, readline, sys
from datetime import datetime, timedelta
from collections import Counter, defaultdict
from functools import partial
import matplotlib.pyplot as plt

locale.resetlocale()
readline.parse_and_bind("tab: complete")

def get_threads_sizes(path):

  threads = {}

  for element in os.listdir(dir_name):
    sub_dir = os.path.join(dir_name, element)

    if not os.path.isdir(sub_dir):
      continue

    if 'message_1.json' in os.listdir(sub_dir):
      size = sum(os.path.getsize(os.path.join(sub_dir, e)) for e in os.listdir(sub_dir))
      threads[sub_dir] = size

  return threads


def get_thread_data(thread_path):

  thread_data = None

  fix_utf8 = partial(re.compile(rb'\\u00([\da-f]{2})').sub, lambda m: bytes.fromhex(m.group(1).decode()))

  for file_name in os.listdir(thread_path):
    if not re.search(r'message_\d+\.json', file_name):
      continue

    with open(os.path.join(thread_path,file_name), 'rb') as thread:

      clean = fix_utf8(thread.read()).decode('utf-8')
      file_data = json.loads(clean)

      if thread_data is None:
        thread_data = file_data

      else:
        thread_data['messages'].extend(file_data['messages'])

  return thread_data


def get_title(*, path=None, data=None):

  title = None

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    title = data['title']

  return title


def get_participants(*, path=None, data=None):

  participants = []

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    participants = [person['name'] for person in data['participants']]

  return participants


def get_message(idx, *, path=None, data=None):

  msg = {}

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    messages = sorted(data['messages'], key=lambda m:m['timestamp_ms'])
    if 0 <= idx < len(messages):
      msg = messages[idx]

  return msg


def get_timestamps(*, path=None, data=None):

  timestamps = []

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    for msg in data['messages']:
      timestamps.append(msg['timestamp_ms']/1000)

  return timestamps


def get_time_info(*, path=None, data=None):

  timestamps = get_timestamps(path=path, data=data)

  start = min(timestamps)
  last  = max(timestamps)

  return datetime.fromtimestamp(start), timedelta(seconds=last-start)


def get_number_of_messages(*, path=None, data=None):

  participants = get_participants(path=path, data=data)
  person_count = {person:0 for person in participants}

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    for msg in data['messages']:
      person_count[msg['sender_name']] += 1

  total_count = sum(person_count.values())
  return total_count, person_count


def get_number_of_words(*, path=None, data=None):

  participants = get_participants(path=path, data=data)
  person_count = {person:0 for person in participants}

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    for msg in data['messages']:
      if 'content' in msg:
        person_count[msg['sender_name']] += len(msg['content'].split())

  total_count = sum(person_count.values())
  return total_count, person_count


def get_longest_message(*, path=None, data=None):

  participants = get_participants(path=path, data=data)
  longest_msg = {person:{'content':''} for person in participants}

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    for msg in data['messages']:
      sender = msg['sender_name']
      if 'content' in msg:
        if len(msg['content']) >= len(longest_msg[sender]['content']):
          longest_msg[sender] = msg

  return longest_msg


def get_most_used_words(*, path=None, data=None):

  participants = get_participants(path=path, data=data)
  used_words = {person:Counter() for person in participants}

  if path is not None:
    data = get_thread_data(path)

  if data is not None:

    for msg in data['messages']:
      sender = msg['sender_name']
      if 'content' in msg:
        used_words[sender].update(msg['content'].lower().split())

  return used_words


def get_most_different_words(used_words):

  word_diff = {person:{} for person in used_words}

  common_words = set()
  for words in used_words.values():
    if len(common_words) == 0:
      common_words |= words.keys()
    else:
      common_words &= words.keys()

  common_count = {}
  for person in used_words:
    common_count[person] = sum(used_words[person][word] for word in common_words)

  for person in used_words:
    for word in common_words:
      usage = used_words[person][word] / common_count[person]

      other_word = other_tot = 0
      for other_person in used_words:
        if other_person != person:
          other_word += used_words[other_person][word]
          other_tot  += common_count[other_person]
      other_usage = other_word / other_tot

      word_diff[person][word] = usage / other_usage

  return word_diff


def get_active_hours(*, path=None, data=None):

  hours = {h:0 for h in range(24)}

  timestamps = get_timestamps(path=path, data=data)

  for tmstp in timestamps:
    send_time = datetime.fromtimestamp(tmstp)
    hours[send_time.hour] += 1

  hours = {h:c/sum(hours.values()) for h,c in hours.items()}

  return hours


def get_active_day_of_week(*, path=None, data=None):

  days = {d:0 for d in range(7)}

  timestamps = get_timestamps(path=path, data=data)

  for tmstp in timestamps:
    send_time = datetime.fromtimestamp(tmstp)
    days[send_time.weekday()] += 1

  days = {d:c/sum(days.values()) for d,c in days.items()}

  return days


def get_most_active_24h(*, path=None, data=None):

  timestamps = sorted(get_timestamps(path=path, data=data))

  best_start = best_count = 0
  cur_range = [timestamps[0]]

  for tmstp in timestamps[1:]:

    if len(cur_range) >= best_count:
      best_start = cur_range[0]
      best_count = len(cur_range)

    while cur_range and timedelta(seconds=tmstp - cur_range[0]).days >= 1:
      cur_range.pop(0)

    cur_range.append(tmstp)

  return datetime.fromtimestamp(best_start), best_count


def get_biggest_pause(*, path=None, data=None):

  timestamps = sorted(get_timestamps(path=path, data=data))

  start_idx = 0
  msg = get_message(start_idx, path=path, data=data)
  if 'content' in msg and msg['content'].startswith('Say hi to your new Facebook friend,'):
    start_idx += 1

  best_start = best_end = timestamps[start_idx]

  for idx in range(start_idx+1,len(timestamps)):

    if timestamps[idx]-timestamps[idx-1] >= best_end-best_start:
      best_start, best_end = timestamps[idx-1], timestamps[idx]

  return datetime.fromtimestamp(best_start), datetime.fromtimestamp(best_end)


def get_number_per_week(*, path=None, data=None):

  timestamps = sorted(get_timestamps(path=path, data=data))

  weeks_count = defaultdict(int)

  start = timestamps[0]
  for tmstp in timestamps:
    weeks_from_start = timedelta(seconds=tmstp-start).days // 7
    weeks_count[weeks_from_start] += 1

  start = datetime.fromtimestamp(start)

  total_weeks = max(weeks_count)+1
  if total_weeks // 52 > 3:
    lbl_f = '{:%Y}'
  else:
    lbl_f = '{:%b %Y}'
  weeks_label = [lbl_f.format(start+timedelta(days=7*w)) for w in range(total_weeks)]
  weeks_label = [weeks_label[0]] + [w if w != v else '' for v,w in zip(weeks_label[:-1],weeks_label[1:])]

  return weeks_count, weeks_label


if __name__ == '__main__':

  dir_name = input('Path of your facebook download: ')

  dir_name = os.path.join(dir_name, 'messages', 'inbox')

  if not os.path.isdir(dir_name):
    print(f'Directory {dir_name} does not exist')
    sys.exit()

  threads = get_threads_sizes(dir_name)
  print(f'Found {len(threads)} threads\n')

  nb_show = int(input(f'Number of threads to show [1-{len(threads)}]: '))
  most_popular = sorted(threads, key=threads.get, reverse=True)[:nb_show]

  threads_by_participants = {}
  for i, thread in enumerate(most_popular):
    print(f'{i:2d}: {get_title(path=thread)}')

  select = int(input(f'\nSelect thread [0-{nb_show-1}]: '))

  sel_thread = most_popular[select]

  sel_data = get_thread_data(sel_thread)

  sel_title = get_title(data=sel_data)
  sel_participants = get_participants(data=sel_data)
  sel_start, sel_duration = get_time_info(data=sel_data)
  sel_mcount, sel_part_mcount = get_number_of_messages(data=sel_data)
  sel_wcount, sel_part_wcount = get_number_of_words(data=sel_data)
  sel_24h_start, sel_24h_count = get_most_active_24h(data=sel_data)
  sel_start_long, sel_end_long = get_biggest_pause(data=sel_data)
  sel_week_rep = get_active_day_of_week(data=sel_data)
  sel_day_rep = get_active_hours(data=sel_data)
  sel_weeks_count, sel_weeks_label = get_number_per_week(data=sel_data)
  sel_words = get_most_used_words(data=sel_data)
  sel_personal_words = get_most_different_words(sel_words)
  sel_longest = get_longest_message(data=sel_data)



  print()
  print(f'Title:                {sel_title}')
  print(f'Participants:         {", ".join(sel_participants)}')
  print(f'Since:                {sel_start:%a, %d %b %Y}')
  print(f'Total messages sent:  {sel_mcount} messages ~ {sel_mcount/sel_duration.days:.1f} msg per day')
  print(f'Most active 24 hours: Starting on {sel_24h_start:%a, %d %b %Y at %H:%M:%S} with {sel_24h_count} messages')
  print(f'Longest pause:        From {sel_start_long:%d %b %Y} to {sel_end_long:%d %b %Y} for {timedelta(seconds=(sel_end_long-sel_start_long).total_seconds()).days} days')

  print('\nWeek activity:')
  for day in sel_week_rep:
    name_of_day = calendar.day_name[day].ljust(max(map(len,calendar.day_name)), ' ')
    print(f'{name_of_day}: ({100*sel_week_rep[day]:5.2f}%) {"#"*int(100*sel_week_rep[day])}')

  print('\nDay activity:')
  for hour in sel_day_rep:
    print(f'{hour:02d}: ({100*sel_day_rep[hour]:5.2f}%) {"#"*int(100*sel_day_rep[hour])}')

  print('\nMost active:')
  for person in sel_participants:
    first_name, *_ = person.split()
    mcount = sel_part_mcount[person]
    wcount = sel_part_wcount[person]
    print(f'{" "*15}{first_name}\t: {mcount} msg ({100*mcount/sel_mcount:.1f}%) ~ {wcount} wrd ({100*wcount/sel_wcount:.1f}%) ~ {wcount/mcount:.1f} wpm')

  print('\nMost used words:')
  for person in sel_participants:
    first_name, *_ = person.split()
    print(f'{" "*15}{first_name}\t: '+', '.join(word for word, _ in sel_words[person].most_common(10)))

  print('\nMost personal words:')
  for person in sel_participants:
    first_name, *_ = person.split()
    best_words = sorted(sel_personal_words[person], key=sel_personal_words[person].get, reverse=True)[:5]
    print(f'{" "*15}{first_name}\t: '+', '.join(f'{word} (x{sel_personal_words[person][word]:.0f})' for word in best_words))

  print('\nLongest messages:')
  for person in sel_participants:
    first_name, *_ = person.split()
    length = len(sel_longest[person]['content'].split())
    send_time = datetime.fromtimestamp(sel_longest[person]['timestamp_ms']/1000)
    print(f'{" "*15}{first_name}\t: {length} words on {send_time:%a, %d %b %Y at %H:%M:%S}, show? [Y/N]: ',end='')
    if 'y' in input().lower(): print(sel_longest[person]['content'],end='\n\n')

  x = [w for w in range(max(sel_weeks_count)+1)]
  y = [sel_weeks_count[w] for w in x]
  plt.bar(x,y,tick_label=sel_weeks_label)
  plt.title('Number of messages per week')
  plt.xticks([i for i,l in enumerate(sel_weeks_label) if l],[l for l in sel_weeks_label if l],rotation=45)
  plt.xlim((x[0]-1, x[-1]+1))
  plt.tight_layout()
  plt.show()
