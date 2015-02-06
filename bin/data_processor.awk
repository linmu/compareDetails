BEGIN {
    FS=" "
}
NR == FNR {
    old[$1"#""buy_count"]=$2
    old[$1"#""use_count"]=$3
    old[$1"#""use_cancel_count"]=$4
    old[$1"#""refund_count"]=$5
}
NR > FNR {
    new[$1"#""buy_count"]=$2
    new[$1"#""use_count"]=$3
    new[$1"#""use_cancel_count"]=$4
    new[$1"#""refund_count"]=$5
}
END {
    for(item in old){
        if(new[item] == "" || old[item] != new[item]){
            split(item,arr,"#")
            printf("%s|%s mismatch|old-%s, new-%s\n",arr[1],arr[2],old[item],new[item])
        }
        delete old[item]
        delete new[item]
    }
    
    for(item in new){
        if(old[item] == "" || new[item] != old[item]) {
            split(item,arr,"#")
            printf("%s|%s mismatch|old-%s,new-%s\n",arr[1],arr[2],old[item],new[item])
        }
        delete new[item]
        delete old[item]
    }
}
