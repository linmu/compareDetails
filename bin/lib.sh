#!/bin/bash

function execMySql2Array()
{
    if [ $# -ne 2 ];then
        echo "Please input two paras: DB_NAME YOUR_SQL" 
        exit
    fi
    case $1 in
        $AUTOPAY_OLD)
            ret_array=(`mysql -h$DB_AUTOPAY_HOST -P$DB_AUTOPAY_PORT -u$DB_AUTOPAY_USER -p$DB_AUTOPAY_PWD $1 -N -e "$2"`)
            ;;
        $AUTOPAY_NEW)
            ret_array=(`mysql -h$DB_AUTOPAY_HOST -P$DB_AUTOPAY_PORT -u$DB_AUTOPAY_USER -p$DB_AUTOPAY_PWD $1 -N -e "$2"`)
            ;;
        $DETAIL)
            ret_array=(`mysql -h$DB_DETAIL_HOST -P$DB_DETAIL_PORT -u$DB_DETAIL_USER -p$DB_DETAIL_PWD $1 -N -e "$2"`)
            ;;
        *)
            ;;
    esac
}

function execMySql2File()
{
    if [ $# -ne 4 ];then
        echo "Please input four paras: DB_NAME YOUR_SQL OUTPUT_FILE_NAME IFAPPEND"
        exit
    fi
    if [ "X$4" = "XappendTrue" ];then
        case $1 in
            $AUTOPAY_OLD)
                mysql -h$DB_AUTOPAY_HOST -P$DB_AUTOPAY_PORT -u$DB_AUTOPAY_USER -p$DB_AUTOPAY_PWD $1 -N -e "$2" >> $3
                ;;
            $AUTOPAY_NEW)
                mysql -h$DB_AUTOPAY_HOST -P$DB_AUTOPAY_PORT -u$DB_AUTOPAY_USER -p$DB_AUTOPAY_PWD $1 -N -e "$2" >> $3
                ;;
            $DETAIL)
                mysql -h$DB_DETAIL_HOST -P$DB_DETAIL_PORT -u$DB_DETAIL_USER -p$DB_DETAIL_PWD $1 -N -e "$2" >> $3
                ;;
            *)
                ;;
        esac
    elif [ "X$4" = "XappendFalse" ];then
        case $1 in
            $AUTOPAY_OLD)
                mysql -h$DB_AUTOPAY_HOST -P$DB_AUTOPAY_PORT -u$DB_AUTOPAY_USER -p$DB_AUTOPAY_PWD $1 -N -e "$2" > $3
                ;;
            $AUTOPAY_NEW)
                mysql -h$DB_AUTOPAY_HOST -P$DB_AUTOPAY_PORT -u$DB_AUTOPAY_USER -p$DB_AUTOPAY_PWD $1 -N -e "$2" > $3
                ;;
            $DETAIL)
                mysql -h$DB_DETAIL_HOST -P$DB_DETAIL_PORT -u$DB_DETAIL_USER -p$DB_DETAIL_PWD $1 -N -e "$2" > $3
                ;;
            *)
                ;;
        esac
    else
        echo "wrong redirection flag"
        exit
    fi
}

function getRecord2File()
{
    if [ $# -ne 6 ];then
        echo "Please input three paras: INPUT_FILE_NAME OUTPUT_FILE_NAME GROUP_SIZE DB_NAME YOUR_SQL REPLACE_INDEX"
        exit
    fi
    
    local id_file=$1
    local record_file=$2
    local id_num=`wc -l $id_file | awk -F' ' '{print $1}'`
    local group_size=$3
    local sub_size=$((($id_num+$group_size-1)/$group_size))

    local begin=1
    local end=$(($begin+$group_size-1))
    local db_name=$4
    local sql=$5
    local index=$6
    for((i=1;i<=$sub_size;i++))
    do
        sub_list=`sed -n "${begin},${end}p" $id_file | tr -t '\n' ',' | sed -e 's/,$//g'`
        sql=${sql//$index/$sub_list}
        execMySql2File "$db_name" "$sql" "$record_file" "appendTrue"
	    sleep 0.1
        begin=$(($begin+$group_size))
        end=$(($end+$group_size))
	index=$sub_list
    done  
}


function formatFile()
{
   sed -e 's/ /\n/g' $1 > $2
   rm -rf $1   
}

function sendEmail()
{
    IFS=";"
    local from="nuomitongzhi@baidu.com"
    local to="linmu@baidu.com"
    local subject="$2"
    local body=`cat $1`
    local content_type="Content-type:text/plain;charset=gb2312"
    local mail_content="to:${to}\nfrom:${from}\nsubject:${subject}\n${content_type}\n${body}"
    echo  -e ${mail_content} | /usr/sbin/sendmail -t
}
