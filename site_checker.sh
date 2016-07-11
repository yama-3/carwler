#!/bin/sh
. /home/yamabe/work/site_checker/bin/activate
python /home/yamabe/work/site_checker/Src/test_site_checker.py tudou http://www.soku.com/t/nisearch/AKB48/_cid__time__sort_score_display_album_high_0_page_1
deactivate

sudo rsync -ur --delete /home/yamabe/work/site_checker/Src/work/ /mnt/cs-fs3/wf/stats/site_check
