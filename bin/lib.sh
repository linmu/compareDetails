#!/bin/bash

############################
##! @Author:Mu Lin
##! @Date:2014-02-01
##! @TODO:public functions
############################
FUNC_SUCC=0
FUNC_ERROR=1

function getTime()
{
    echo "$(date '+%Y-%m-%d %H:%M:%S')"
}

function printMsg()
{
    echo "$1"
}

function loginfo()
{
    echo "`getTime` [`caller 0 | awk -F' ' '{print $1,$2}'`] $1" >> $LOG_FILE
}

function failExit()
{
    loginfo "Error: $1, exited, please check problem"
    exit $FUNC_ERROR
}

#extract record from DB to a global array variable
#input: dbname yoursql
function execMySql2Array()
{
    if [[ $# -ne 2 ]];then
        loginfo "need params"
        failExit "execMySql2Array invalid params [$*]"
    fi

    local ret
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
	    loginfo "cannot find database $1, please check"
	    failExit "wrong database name $1"
            ;;
    esac

    ret=$?
    if [[ $ret -eq $FUNC_SUCC ]];then
        loginfo "apply sql $2 on database $1 successfully"
	return $ret
    else
        failExit "apply sql $2 on database $1 failed"
    fi
}

#extract record from DB to a file
#input: dbname yoursql outputfile appendflag
function execMySql2File()
{
    if [[ $# -ne 4 ]];then
        loginfo "need params"
        failExit "execMySql2File invalid params [$*]"
    fi

    local ret
    if [[ "$4" = appendTrue ]];then
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
		loginfo "cannot find database $1, please check"
		failExit "wrong database name $1"
                ;;
        esac
    elif [[ "$4" = appendFalse ]];then
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
		loginfo "cannot find databases $1, please check"
		failExit "wrong database name $1"
                ;;
        esac
    else
        loginfo "do not support append flag $4, please check"
        failExit "wrong append flag $4"
    fi

    ret=$?
    if [[ $ret -eq $FUNC_SUCC ]];then
        loginfo "apply sql $2 on database $1 successfully"
        return $ret
    else
        failExit "apply sql $2 on database $1 failed"
    fi
}

#batch get record from database to a file
#input: inputfile outputfile groupsize dbname yoursql replaceindex
function getRecord2File()
{
    if [[ $# -ne 6 ]];then
        loginfo "need params"
        failExit "getRecord2File invalid params [$*]"
    fi
    
    local input_file=$1
    local output_file=$2
    local record_num=`wc -l ${input_file} | awk -F' ' '{print $1}'`
    local group_size=$3
    local sub_size=$(((${record_num}+${group_size}-1)/${group_size}))

    local begin=1
    local end=$((${begin}+${group_size}-1))
    local db_name=$4
    local sql=$5
    local index=$6
    for((i=1;i<=${sub_size};i++))
    do
        sub_list=`sed -n "${begin},${end}p" $input_file | tr -t '\n' ',' | sed -e 's/,$//g'`
        sql=${sql//$index/${sub_list}}
        execMySql2File "$db_name" "$sql" "$output_file" "appendTrue"
	sleep 0.1
        begin=$(($begin+${group_size}))
        end=$(($end+${group_size}))
	index=${sub_list}
    done  
}

#input: inputfile outputfile
function formatFile()
{
    if [[  $# -ne 2 ]];then
        loginfo "need params"
        failExit "formatFile invalid params [$*]"
    fi

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
