#!/usr/bin/env python
from operator import itemgetter
import sys
import re
current_word = None
current_count = 0

for line in sys.stdin:
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    # clear regular expression
    word = re.compile(r'\W+', re.UNICODE).split(word)[0]
    count = int(count)

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            print '%s\t%s' % (current_word, current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
print '%s\t%s' % (current_word, current_count)