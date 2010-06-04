#!/bin/bash

# 
# mkuser.sh - Linux and Darwin-compliant program to add users.
# 
# If invoked as mksystemuser.sh (via a symlink to mkuser.sh) a system user will be created without a password
#    
# author:  cwoerner@demandbase.com
# date:    Thu May 21 20:15:01 PDT 2009
#

progname=`basename $0`;

mksystemuser=mksystemuser.sh
mkuser=mkuser.sh

os=`uname -s`;
if [ "xLinux" != "x$os" ] && [ "xDarwin" != "x$os" ]; then 
    echo "unsupported host operating system `uname -a`";
    exit 1; 
fi;

# if rbash doesn't exist, create it
bash=`which bash`;
rbash=`which rbash`;
if [ -z "$rbash" ]; then 
    rbash=`echo $bash | sed 's/bash/rbash/'`;
    ln -s $bash $rbash; 
fi;

# if rbash isn't a shell, make it so
if [ -e /etc/shells ]; then
    rbash_shell=`grep $rbash /etc/shells`;
    if [ -z "$rbash_shell" ]; then
	echo $rbash >> /etc/shells;
    fi;
fi;

username=$1;
if [ -z "$username" ]; then 
    echo "missing username"; 
    exit 1; 
fi;

if [ "xroot" == "$username" ]; then
    echo "root user should already exist";
    exit 0;
fi;

groupname=$2;
if [ -z "$groupname" ]; then
    echo "missing groupname";
    exit 1;
fi;


if [ $progname == $mksystemuser ]; then
    additional_groups=$3;
    homedir=$4;
    shell=/sbin/nologin;
    [ -x "$shell" ] || shell=/sbin/false;
    [ -x "$shell" ] || shell=/sbin/true;
    [ -x "$shell" ] || shell=$rbash;
else
    password=$3;
    additional_groups=$4;
    homedir=$5;
    shell=$bash;
fi;

if [ ! -z "$additional_groups" ]; then addl_group_arg="-G $additional_groups"; fi;

function add_user_to_group() { 
    #
    # some forms of useradd don't allow adding a user to additional groups a-la "-a -G grouplist"
    #

    username=$1;
    additional_group=$2;
    groupadd=$3; 

    echo "additional_group=$additional_group";
    tmpgrouplist=`echo $additional_group | sed 's/,/ /g'`;

    echo "tmpgrouplist=$tmpgrouplist";
    for g in $tmpgrouplist; do 
	if [ -z "`grep ^$g: /etc/group`" ]; then
	    echo "adding group $g";	    
	    msg=`$groupadd $g`;
	    rc=$?; if [ $rc -ne 0 ]; then echo "failed to add additional group $g ($groupadd $g): $msg"; exit $rc; fi;
	fi;
    done;

    echo "adding user $username to group $additional_group using $groupadd";

    lgroupmod=`which lgroupmod`;

    if [ ! -z "$lgroupmod" ]; then
	for g in $tmpgrouplist; do 
	    ingroup=false;
	    for g2 in `groups $username`; do 
		if [ $g2 = $g ]; then
		    ingroup=true;
		fi;
	    done;
	    if [ $ingroup = false ]; then
		msg=`lgroupmod -M $username $g`;
		rc=$?;
		if [ $rc -ne 0 ]; then
		    echo "failed to add $username to $g (lgroupmod -M $username $g): $msg";
		    exit $rc;
		fi;
	    fi;
	done;
    else
	msg=`usermod -a -G $additional_group $username`;
	rc=$?;
	if [ $rc -ne 0 ]; then
	    echo "failed to add $username to $additional_group (usermod -a -G $additional_group $username): $msg";
	    exit $rc;
	fi;
    fi;
};

if [ "x$os" == "xLinux" ]; then 

	if [ -z "$groupadd" ]; then groupadd=`which lgroupadd`; fi;
	if [ -z "$groupadd" ]; then groupadd=`which groupadd`; fi;
	if [ -z "$groupadd" -a -x "/usr/sbin/lgroupadd" ]; then groupadd="/usr/sbin/lgroupadd"; fi;
	if [ -z "$groupadd" -a -x "/sbin/lgroupadd" ]; then groupadd="/sbin/lgroupadd"; fi;
	if [ -z "$groupadd" -a -x "/usr/sbin/groupadd" ]; then groupadd="/usr/sbin/groupadd"; fi;
	if [ -z "$groupadd" -a -x "/sbin/groupadd" ]; then groupadd="/sbin/groupadd"; fi;
	if [ -z "$groupadd" -o ! -x $groupadd ]; then echo "failed to find suitable (l)groupadd"; exit 1; fi;
	if [ ! -f /etc/gshadow ]; then
	    # stupid groupadd,lgroupadd will add group but return nonzero error code if gshadow doesn't exist!
	    touch /etc/gshadow;
	fi;
		
	if [ -z "$usermod" ]; then usermod=`which lusermod`; fi;
	if [ -z "$usermod" ]; then usermod=`which usermod`; fi;
	if [ -z "$usermod" -a -x "/usr/sbin/lusermod" ]; then usermod="/usr/sbin/lusermod"; fi;
	if [ -z "$usermod" -a -x "/sbin/lusermod" ]; then usermod="/sbin/lusermod"; fi;
	if [ -z "$usermod" -a -x "/usr/sbin/usermod" ]; then usermod="/usr/sbin/usermod"; fi;
	if [ -z "$usermod" -a -x "/sbin/usermod" ]; then usermod="/sbin/usermod"; fi;
	if [ -z "$usermod" -o ! -x $usermod ]; then echo "can't find suitable (l)usermod program"; exit 1; fi;


	if [ -z "$useradd" ]; then useradd=`which luseradd`; fi;
	if [ -z "$useradd" ]; then useradd=`which useradd`; fi;
	if [ -z "$useradd" -a -x "/usr/sbin/luseradd" ]; then useradd="/usr/sbin/luseradd"; fi;
	if [ -z "$useradd" -a -x "/sbin/luseradd" ]; then useradd="/sbin/luseradd"; fi;
	if [ -z "$useradd" -a -x "/usr/sbin/useradd" ]; then useradd="/usr/sbin/useradd"; fi;
	if [ -z "$useradd" -a -x "/sbin/useradd" ]; then useradd="/sbin/useradd"; fi;
	if [ -z "$useradd" -o ! -x $useradd ]; then echo "can't find suitable (l)useradd program"; exit 1; fi;


	gid=`grep ^$groupname: /etc/group | awk -F: '{print $3}'`;
	if [ -z "$gid" ]; then
		echo "adding group $groupname";

		msg=`$groupadd $groupname`;
		rc=$?;
		if [ $rc -ne 0 ]; then
		    echo "failed to add group ($groupadd $groupname): $msg";
		    exit $rc;
		fi;
	fi;

	gid=`grep ^$groupname: /etc/group | awk -F: '{print $3}'`; 
	if [ -z "$gid" ]; then
		echo "failed to create group $groupname";
		exit 1; 
	fi;

	if [ -z "$homedir" ]; then
	    if [ -z "$password" ]; then
		homedir="";
	    else
	        homedir="-d /home/$username";
	    fi;
	else
	    homedir="-d $homedir";
	fi;

	if [ -z "`grep ^$username: /etc/passwd`" ]; then 

		echo "creating user $username";

		password_opt=;
		if [ -z "$password" ]; then
		    password_opt="-r";
		else 
		    if [ $password = '!!' ]; then
			password=;
			password_opt=;
		    else 
			password_opt=;
		    fi
		fi

		echo "adding user ($useradd -c 'demandbase system user' $homedir -g $gid $password_opt -s $shell $username) ..."
	     	msg=`$useradd -c "demandbase system user" $homedir -g $gid $password_opt -s $shell $username`;
		rc=$?;
		if [ $rc -ne 0 ]; then
		    echo "failed to add user ($useradd -c 'demandbase system user' $homedir -g $gid $password_opt -s $shell $username): $msg";
		    exit $rc;
		fi;

		echo "done."

		if [ ! -z "$password" ]; then
		    echo "setting password for $username ...";
		    echo $password | passwd --stdin $username
		    echo "done."
		fi;

		add_user_to_group "$username" "$additional_groups" $groupadd;

	else		
		echo "modifying user ($usermod -c 'demandbase system user' $homedir -g $gid -s $shell $username) ..."
		msg=`$usermod -c "demandbase system user" $homedir -g $gid -s $shell $username`;
		rc=$?;
		if [ $rc -ne 0 ]; then
		    echo "failed to modify user ($usermod  -c 'demandbase system user' $homedir -g $gid -s $shell $username): $msg";
		    exit $rc;
		fi;

		echo "done."

		add_user_to_group "$username" "$additional_groups" $groupadd;
	fi;

else 
    if [ "x$os" == "xDarwin" ]; then

	nopasswd="'*'";
	if [ -z "$password" || $password = '!!' ]; then password=$nopasswd; fi;

	# if the group exists then make sure it has a gid and create one for it if not
	if [ ! -z "`dscl . -search /groups name $groupname`" ]; then
	    gid=`dscl . -list /groups gid | grep ^$groupname | awk '{print $2}'`; 
	    if [ -z "$gid" ]; then 
		echo "group $groupname found with no gid - assigning one now"
		gid=`dscl . -list /groups gid | awk '{print $2}' | sort -n | tail -n 1`;
		if [ $gid -lt 5000 ]; then gid=5000; fi;
		gid=$[ $gid + 1 ]; 
		echo "using gid $gid";
		dscl . -create /groups/$groupname gid $gid;
	    fi;
	fi;

	# if the group doesn't exist, then create it
	if [ -z "$gid" ]; then
	    echo "adding group $groupname";
	    gid=`dscl . -list /groups gid | awk '{print $2}' | sort -n | tail -n 1`;
	    if [ $gid -lt 5000 ]; then gid=5000; fi;
	    gid=$[ $gid + 1 ]; 
	    echo "using gid $gid";
	    dscl . -create /groups/$groupname gid $gid;
	    dscl . -create /groups/$groupname passwd $nopasswd
	fi;
	gid=`dscl . -list /groups gid | grep ^$groupname | awk '{print $2}'`; 
	if [ -z "$gid" ]; then
	    echo "failed to add group $groupname";
	    exit 1;
	fi;

	# create the user if it doesn't exist
	if [ -z "`dscl . -search /users name $username`" ]; then 
	    uid=`dscl . -list /users uid | awk '{print $2}' | sort -n | tail -n 1`;
	    if [ -z "$uid" ]; then echo "no uids found, using default"; uid=0; fi;
	    if [ $uid -lt 5000 ]; then uid=5000; fi;
	    uid=$[ $uid + 1 ]; 
	    echo "using gid $gid";
	    dscl . -create /users/$username uid $uid
	    dscl . -create /users/$username gid $gid
	    dscl . -create /users/$username shell $shell
	    dscl . -create /users/$username passwd $password;
	    dscl . -create /users/$username realname "Demandbase System User"
	fi;
    else
	echo "unsupported os (`uname`)"; 
	exit 1; 
    fi;
fi;
