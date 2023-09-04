DB_PATH="/data/0/xiongyiqin.xyq/observer1"
TID="$1"
# grep "$TID" "$DB_PATH/log/observer.log*" > temp.txt
grep "$TID" $DB_PATH/log/observer.log* | grep "column_decoder->decoder_->get_type" -m 1 --color=always | grep "is_sorted_dict" --color=always
