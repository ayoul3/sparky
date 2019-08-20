echo -e "Following is the MetaData for this EC2 instance: \n"
EC2meta=$(curl -s http://127.0.0.1/latest/meta-data/)

while read line
do
    echo $line
    echo -e "$(curl -s http://169.254.169.254/latest/meta-data/$line/)"
done <<< "$EC2meta"
echo -e "\n"
