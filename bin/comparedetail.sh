#!/bin/bash
################## load public funnction #####################
source ./lib.sh
################## loda public function end ##################

function main()
{
    if [ -n "$1" ];then
        TODAY=$1
        OLD_QUERY_DAY=`date -d "-1 day $TODAY" +%Y%m%d`
        NEW_QUERY_DAY=`date -d $OLD_QUERY_DAY +"%Y-%m-%d"`
    else
        TODAY=`date +%Y%m%d`
        OLD_QUERY_DAY=`date -d last-day +%Y%m%d`
        NEW_QUERY_DAY=`date -d $OLD_QUERY_DAY +"%Y-%m-%d"`
    fi

    ROOT_DIR=`cd .. && pwd`
    DATA_DIR="$ROOT_DIR/data"
    LOG_DIR="$ROOT_DIR/logs"
    QUEUEID_DEALID_FILE="$DATA_DIR/queueid_dealid_$TODAY"
    DEALID_FILE="$DATA_DIR/dealid_$TODAY"
    QUEUEID_FILE="$DATA_DIR/queueid_$TODAY"
    OLD_TODAY_DEALID="$DATA_DIR/old_today_dealid_$TODAY"
    NEW_TODAY_QUEUEID="$DATA_DIR/new_today_queueid_$TODAY"
    OLD_DETAIL_RECORD="$DATA_DIR/old_detail_record_$TODAY"
    NEW_DETAIL_RECORD="$DATA_DIR/new_detail_record_$TODAY"
    DIFFILE="$LOG_DIR/diff_result_file_$TODAY"
    GROUP_SIZE=100
#    
############################## prepare Sqls ####################################    
    IDS_INDEX="<dealids>"

    SQL_DEALIDS_OLD="select deal_id from stat_day_option where date=$OLD_QUERY_DAY and deal_id in ($IDS_INDEX) order by deal_id"

    SQL_QUEUEIDS_NEW="select distinct queue_id from settlement_detail where opt_time >= '$NEW_QUERY_DAY 00:00:00' and opt_time <= '$NEW_QUERY_DAY 23:59:59' and queue_id in ($IDS_INDEX)"

    SQL_DETAILS_OLD="select deal_id,ifnull(sum(buy_count),0),ifnull(sum(use_count),0),ifnull(sum(use_cancel_count),0),ifnull(sum(refund_count),0) from stat_day_option where date=$OLD_QUERY_DAY and deal_id in ($IDS_INDEX) group by deal_id order by deal_id"

    SQL_DETAILS_NEW="select product_id,sum(case when detail_type = 1 then 1 else 0 end) as buy_count,sum(case when detail_type = 2 then 1 else 0 end) as use_count, sum(case when detail_type = 3 then 1 else 0 end) as use_cancel_count, sum(case when detail_type = 4 then 1 else 0 end) as refund_count,queue_id from settlement_detail where opt_time >= '$NEW_QUERY_DAY 00:00:00' and opt_time <= '$NEW_QUERY_DAY 23:59:59' and queue_id in ($IDS_INDEX) group by queue_id"
############################ prepare Sqls done #################################

    if [ ! -d $DATA_DIR ];then
        mkdir -p $DATA_DIR
    fi
    
    if [ ! -d $LOG_DIR ];then
        mkdir -p $LOG_DIR
    fi

############################## delete exist files start ##############################
    if [ -f $QUEUEID_DEALID_FILE ];then
        rm -rf $QUEUEID_DEALID_FILE
    fi

    if [ -f $QUEUEID_FILE ];then
	rm -rf $QUEUEID_FILE
    fi

    if [ -f $DEALID_FILE ];then
	rm -rf $DEALID_FILE
    fi

    if [ -f $OLD_TODAY_DEALID ];then
        rm -rf $OLD_TODAY_DEALID
    fi

    if [ -f $NEW_TODAY_QUEUEID ];then
        rm -rf $NEW_TODAY_QUEUEID
    fi

    if [ -f $OLD_DETAIL_RECORD ];then
        rm -rf $OLD_DETAIL_RECORD
    fi

    if [ -f $NEW_DETAIL_RECORD ];then
        rm -rf $NEW_DETAIL_RECORD
    fi

    if [ -f $DIFFILE ];then
        rm -rf $DIFFILE
    fi
############################# delete exist files end ##############################

############################# select new dealids list ##############################
    execMySql2File $AUTOPAY_OLD "select id,deal_id from contract_queue where sys_type = 2 order by deal_id" $QUEUEID_DEALID_FILE "appendFalse"
    
    if [ ! -f $QUEUEID_DEALID_FILE ] || [ ! -s $QUEUEID_DEALID_FILE ];then
        echo "there is no new deals!"
        exit
    fi

    awk -F' ' '{print $1}' $QUEUEID_DEALID_FILE > $QUEUEID_FILE
    awk -F' ' '{print $2}' $QUEUEID_DEALID_FILE > $DEALID_FILE
############################## select new dealids list end ##############################
 
############################## select today's dealids from new and old detail table ##############################
    getRecord2File "$DEALID_FILE" "$OLD_TODAY_DEALID" $GROUP_SIZE "$AUTOPAY_OLD" "$SQL_DEALIDS_OLD" "$IDS_INDEX"
    getRecord2File "$QUEUEID_FILE" "$NEW_TODAY_QUEUEID" $GROUP_SIZE "$DETAIL" "$SQL_QUEUEIDS_NEW" "$IDS_INDEX"
############################### select today's dealids from new and old detail table end  ##############################
#
############################### select today's detail records from new and old detail table ##############################
    getRecord2File "$OLD_TODAY_DEALID" "$OLD_DETAIL_RECORD" $GROUP_SIZE "$AUTOPAY_OLD" "$SQL_DETAILS_OLD" "$IDS_INDEX" 
    getRecord2File "$NEW_TODAY_QUEUEID" "$NEW_DETAIL_RECORD" $GROUP_SIZE "$DETAIL" "$SQL_DETAILS_NEW" "$IDS_INDEX"
############################### select today's dealids records from new and old detail table end  ##############################

    sort -n -k1 $NEW_DETAIL_RECORD -o $NEW_DETAIL_RECORD

    if [ ! -f $DIFFILE ];then
        cd "$LOG_DIR"
        touch ${DIFFILE##*/}
    fi
############################### compare detail record begin ##############################
    old_file_lines=`wc -l $OLD_DETAIL_RECORD | awk -F' ' '{print $1}'`
    new_file_lines=`wc -l $NEW_DETAIL_RECORD | awk -F' ' '{print $1}'`
    line_count_new=1
    echo "compare detail record begin!"
    for((line_count_old=1;$line_count_old<=$old_file_lines;))
    do
        old_detail_array=(`sed -n ${line_count_old}p $OLD_DETAIL_RECORD`)
        new_detail_array=(`sed -n ${line_count_new}p $NEW_DETAIL_RECORD`)
        if [ ${#new_detail_array[*]} -eq 0 ];then
            break
        fi
        echo "processing old line no ${line_count_old} new line no ${line_count_new} old_dealid:${old_detail_array[0]} new_dealid:${new_detail_array[0]} ..."
        
        echo "new dealid:${new_detail_array[0]} old dealid:${old_detail_array[0]}"
        if [ ${new_detail_array[0]} -ne ${old_detail_array[0]} ];then
            if [ ${new_detail_array[0]} -gt ${old_detail_array[0]} ];then
		    echo "old dealid:${old_detail_array[0]} is not in new dealids list" >> $DIFFILE
                    line_count_old=$(($line_count_old+1))
            elif [ ${new_detail_array[0]} -lt ${old_detail_array[0]} ];then
		    echo "new dealid:${new_detail_array[0]} is not in old dealids list" >> $DIFFILE
		    line_count_new=$(($line_count_new+1))
            fi
            continue
        fi
    
        echo "new buy_count:${new_detail_array[1]} old buy_count:${old_detail_array[1]}"
        if [ ${new_detail_array[1]} -ne ${old_detail_array[1]} ];then
            echo "buy count mismatch|new-${new_detail_array[1]} old-${old_detail_array[1]}|dealId:${new_detail_array[0]}">>$DIFFILE
	    line_count_old=$(($line_count_old+1))
            line_count_new=$(($line_count_new+1))
            continue
        fi
    
        echo "new use_count:${new_detail_array[2]} old use_count:${old_detail_array[2]}"
        if [ ${new_detail_array[2]} -ne ${old_detail_array[2]} ];then
            echo "use count mismatch|new-${new_detail_array[2]} old-${old_detail_array[2]}|dealId:${new_detail_array[0]}">>$DIFFILE
            line_count_old=$(($line_count_old+1))
            line_count_new=$(($line_count_new+1))
            continue
        fi
    
        echo "new use_cancel_count:${new_detail_array[3]} old buy_count:${old_detail_array[3]}"
        if [ ${new_detail_array[3]} -ne ${old_detail_array[3]} ];then
            echo "use cancel count mismatch|new-${new_detail_array[3]} old-${old_detail_array[3]}|dealId:${new_detail_array[0]}">>$DIFFILE
            line_count_old=$(($line_count_old+1))
            line_count_new=$(($line_count_new+1))
            continue
        fi
    
        echo "new refund_count:${new_detail_array[4]} old refund_count:${old_detail_array[4]}"
        if [ ${new_detail_array[4]} -ne ${old_detail_array[4]} ];then
            echo "refund count mismatch|new-${new_detail_array[4]} old-${old_detail_array[4]}|dealId:${new_detail_array[0]}">>$DIFFILE
            line_count_old=$(($line_count_old+1))
            line_count_new=$(($line_count_new+1))
            continue
        fi

        line_count_old=$(($line_count_old+1))
        line_count_new=$(($line_count_new+1))

    done

############################## compare detail record end ##############################
    if [ $line_count_new -le $new_file_lines ];then
        for((i=$line_count_new;i<=$new_file_lines;i++))
        do
            new_detail_array=(`sed -n ${i}p $NEW_DETAIL_RECORD`)
            echo "new dealid:${new_detail_array[0]} is not in old dealids list" >> $DIFFILE
        done
    fi

    if [ $line_count_old -le $old_file_lines ];then
        for((i=$line_count_old;i<=$old_file_lines;i++))
        do
            old_detail_array=(`sed -n ${i}p $OLD_DETAIL_RECORD`)
            echo "old dealid:${old_detail_array[0]} is not in new dealids list" >> $DIFFILE
        done
    fi

    echo "compare detail record end!"

	sendEmail $DIFFILE "Compare Details_$TODAY"
	
    exit
}

PROGRAM=`basename $0`
CONF_DIR=`cd ../conf && pwd`

source $CONF_DIR/compareDetails.conf

main $1
