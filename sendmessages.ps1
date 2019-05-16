1..50| ForEach-Object { Start-RSJob { 
  Invoke-Expression 'aws sqs send-message --queue-url https://sqs.us-east-2.amazonaws.com/628654266155/PoCQueue --message-body "$_" --profile alex.kuni4';
}} | Wait-RSJob 

# 1..100 | ForEach-Object {
#   Invoke-Expression 'aws sqs send-message --queue-url https://sqs.us-east-2.amazonaws.com/628654266155/PoCQueue --message-body "$_" --profile alex.kuni4';
# } 