#!/bin/bash

########################
##! @Author:Mu Lin
##! @Date:2015-02-01
##! @TODO:compare records from two databases
########################
PROGRAM=$(basename $0)
VERSION="1.0"
CURDATE=$(date "+%Y%m%d")

cd $(dirname $0)
BIN_DIR=$(pwd)
DEPLOY_DIR=${BIN_DIR%/*}

CONF_DIR=$(cd $DEPLOY_DIR/conf && pwd)
CONF_FILE_NAME=
TODAY=$CURDATE

function usage()
{
    echo "$PROGRAM usage: [-h] [-v] [-c 'config file name'] [-d 'YYYYMMDD'(default today)]"
}

function usage_and_exit()
{
    usage
    exit $1
}

function version()
{
    echo "$PROGRAM version $VERSION"
}

######### handle input parameters ##########
if [[ $# -lt 1 ]];then
    usage_and_exit 1
fi

while getopts :c:d:hv opt
do
    case $opt in
    c) CONF_FILE_NAME=$OPTARG
       ;;
    d) TODAY=$OPTARG
       ;;
    v) version
       exit 0
       ;;
    h) usage_and_exit 0
       ;;
    ':') echo "$PROGRAM -$OPTARG requires an argument" >&2
       usage_and_exit 1
       ;;
    '?') echo "$PROGRAM: invalid option $OPTARG" >&2
       usage_and_exit 1
       ;;
    esac
done
shift $(($OPTIND-1))

###### load configures ######
source ${CONF_DIR}/${CONF_FILE_NAME}

LOG_DIR="${DEPLOY_DIR}/${LOG_PATH}"
LOG_FILE="${LOG_DIR}/${LOG_FILE_NAME}.${CURDATE}"
DATA_DIR="${DEPLOY_DIR}/${DATA_PATH}"
QUEUEID_DEALID_FILE="${DATA_DIR}/${QUEUEID_DEALID_FILE_NAME}_${TODAY}"
DEALID_FILE="${DATA_DIR}/${DEALID_FILE_NAME}_${TODAY}"
QUEUEID_FILE="${DATA_DIR}/${QUEUEID_FILE_NAME}_${TODAY}"
OLD_TODAY_DEALID_FILE="${DATA_DIR}/${OLD_TODAY_DEALID_FILE_NAME}_${TODAY}"
NEW_TODAY_QUEUEID_FILE="${DATA_DIR}/${NEW_TODAY_QUEUEID_FILE_NAME}_${TODAY}"
OLD_DETAIL_RECORD_FILE="${DATA_DIR}/${OLD_DETAIL_RECORD_FILE_NAME}_${TODAY}"
NEW_DETAIL_RECORD_FILE="${DATA_DIR}/${NEW_DETAIL_RECORD_FILE_NAME}_${TODAY}"
DIFF_DIR="${DEPLOY_DIR}/${DIFF_PATH}"
DIFF_FILE="${DIFF_DIR}/${DIFF_FILE_NAME}_${TODAY}"
DATA_PROCESSOR_FILE="${BIN_DIR}/${DATA_PROCESSOR_FILE_NAME}"

if [[ ! -d ${LOG_DIR} ]];then
    mkdir -p ${LOG_DIR}
fi

if [[ ! -d ${DATA_DIR} ]];then
    mkdir -p ${DATA_DIR}
fi

if [[ ! -d ${DIFF_DIR} ]];then
    mkdir -p ${DIFF_DIR}
fi

############################## delete exist files start ##############################
if [[ -f ${QUEUEID_DEALID_FILE} ]];then
    rm -rf ${QUEUEID_DEALID_FILE}
fi

if [[ -f ${QUEUEID_FILE} ]];then
    rm -rf ${QUEUEID_FILE}
fi

if [[ -f $DEALID_FILE ]];then
    rm -rf ${DEALID_FILE}
fi

if [[ -f ${OLD_TODAY_DEALID_FILE} ]];then
    rm -rf ${OLD_TODAY_DEALID_FILE}
fi

if [[ -f ${NEW_TODAY_QUEUEID_FILE} ]];then
    rm -rf ${NEW_TODAY_QUEUEID_FILE}
fi

if [[ -f ${OLD_DETAIL_RECORD_FILE} ]];then
    rm -rf ${OLD_DETAIL_RECORD_FILE}
fi

if [[ -f ${NEW_DETAIL_RECORD_FILE} ]];then
    rm -rf ${NEW_DETAIL_RECORD_FILE}
fi

if [[ -f ${DIFF_FILE} ]];then
    rm -rf ${DIFF_FILE}
fi

################## load public funnction #####################
source ${BIN_DIR}/lib.sh

############################## prepare Sqls ####################################    
OLD_QUERY_DAY=$(date -d "-1 day ${TODAY}" "+%Y%m%d")
NEW_QUERY_DAY=$(date -d "${OLD_QUERY_DAY}" "+%Y-%m-%d")

IDS_INDEX="<dealids>"

SQL_DEALIDS_OLD="select s.deal_id from stat_day_option s,contract_queue c where s.date=${OLD_QUERY_DAY} and s.deal_id = c.deal_id and c.sys_type = 2 order by deal_id"

SQL_QUEUEIDS_NEW="select distinct queue_id from settlement_detail where opt_time >= '${NEW_QUERY_DAY} 00:00:00' and opt_time <= '${NEW_QUERY_DAY} 23:59:59' and queue_id in (${IDS_INDEX})"

SQL_DETAILS_OLD="select deal_id,ifnull(sum(buy_count),0),ifnull(sum(use_count),0),ifnull(sum(use_cancel_count),0),ifnull(sum(refund_count),0) from stat_day_option where date=${OLD_QUERY_DAY} and deal_id in (${IDS_INDEX}) group by deal_id order by deal_id"

SQL_DETAILS_NEW="select product_id,sum(case when detail_type = 1 then 1 else 0 end) as buy_count,sum(case when detail_type = 2 then 1 else 0 end) as use_count, sum(case when detail_type = 3 then 1 else 0 end) as use_cancel_count, sum(case when detail_type = 4 then 1 else 0 end) as refund_count,queue_id from settlement_detail where opt_time >= '${NEW_QUERY_DAY} 00:00:00' and opt_time <= '${NEW_QUERY_DAY} 23:59:59' and queue_id in (${IDS_INDEX}) group by queue_id"
############################ prepare Sqls done #################################


############################# select new dealids list ##############################
printMsg "select dealids from contract_queue"
loginfo "select dealids from contract_queue"
execMySql2File ${AUTOPAY_OLD} "select id,deal_id from contract_queue where sys_type = 2 order by deal_id" ${QUEUEID_DEALID_FILE} "appendFalse"
    
if [[ ! -e ${QUEUEID_DEALID_FILE} ]];then
    loginfo "there is no new deals on date: ${TODAY}"
    failExit "there is no new deals on ${TODAY}"
fi

awk -F' ' '{print $1}' ${QUEUEID_DEALID_FILE} > $QUEUEID_FILE
awk -F' ' '{print $2}' ${QUEUEID_DEALID_FILE} > $DEALID_FILE
############################## select new dealids list end ##############################
 
############################## select today's dealids from new and old detail table ##############################
printMsg "select today's dealids from old detail table..."
loginfo "select today's dealids from old detail table"
execMySql2File "${AUTOPAY_OLD}" "${SQL_DEALIDS_OLD}" "${OLD_TODAY_DEALID_FILE}" "appendFalse"

printMsg "select today's queueids from new detail table..."
loginfo "select today's queueids from new detail table..."
getRecord2File "${QUEUEID_FILE}" "${NEW_TODAY_QUEUEID_FILE}" ${GROUP_SIZE} "${DETAIL}" "${SQL_QUEUEIDS_NEW}" "${IDS_INDEX}"
############################### select today's dealids from new and old detail table end  ##############################

############################### select today's detail records from new and old detail table ##############################
printMsg "select today's detail records from old detail table..."
loginfo "select today's detail records from old detail table"
getRecord2File "${OLD_TODAY_DEALID_FILE}" "${OLD_DETAIL_RECORD_FILE}" ${GROUP_SIZE} "${AUTOPAY_OLD}" "${SQL_DETAILS_OLD}" "${IDS_INDEX}" 

printMsg "select today's detail records from new detail table..."
loginfo "select today's detail records from new detail table"
getRecord2File "${NEW_TODAY_QUEUEID_FILE}" "${NEW_DETAIL_RECORD_FILE}" ${GROUP_SIZE} "${DETAIL}" "${SQL_DETAILS_NEW}" "${IDS_INDEX}"
############################### select today's dealids records from new and old detail table end  ##############################

sort -n -k1 ${NEW_DETAIL_RECORD_FILE} -o ${NEW_DETAIL_RECORD_FILE}

if [[ ! -e ${DIFF_FILE} ]];then
    cd "${DIFF_DIR}"
    touch ${DIFF_FILE##*/}
fi

############################### compare detail record begin ##############################
printMsg "compare detail records begin"
loginfo "compare detail records begin"

if [[ ! -e ${OLD_DETAIL_RECORD_FILE} ]];then
    failExit "${OLD_DETAIL_RECORD_FILE} doesn't exist"
fi

if [[ ! -e ${NEW_DETAIL_RECORD_FILE} ]];then
    failExit "${NEW_DETAIL_RECORD_FILE} doesn't exist"
fi

awk -f "${DATA_PROCESSOR_FILE}" "${OLD_DETAIL_RECORD_FILE}" "${NEW_DETAIL_RECORD_FILE}" > "${DIFF_FILE}"

sort -t"|" -k1n "${DIFF_FILE}" -o "${DIFF_FILE}"

printMsg "compare detail record end"
loginfo "compare detail record end"
############################## compare detail record end ##############################


sendEmail ${DIFF_FILE} "Compare Details_${TODAY}"

exit $FUNC_SUCC
