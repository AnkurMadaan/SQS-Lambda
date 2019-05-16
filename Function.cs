using System;
using System.Collections.Generic;
using System.Threading;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Threading.Tasks;
using Amazon.Lambda.Core;
using System.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace CompetingConsumerLambda
{

    public class Function
    {
        private const string InputQueueUrl = "https://sqs.us-east-2.amazonaws.com/628654266155/PoCQueue";
        private const string OutputQueueUrl = "https://sqs.us-east-2.amazonaws.com/628654266155/YetAnotherQueue";
        private const string RetryCount = "retryCount";

        private const int TimeOutSeconds = 3;
        private const int DelayMillisecondsSeconds = 20;

        private AmazonSQSClient SqSClient => new AmazonSQSClient();

        // acknowledging of message is done by AWS 
        // because lambda is configured to be triggered !
        public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
        {
            //actually sqsEvent.Records.Count = 1
            var message = sqsEvent.Records.First();
            context.Logger.Log($" ** starting new lambda instance at  {DateTime.Now.ToLongTimeString()} ** ");
            context.Logger.Log($"trying to handle message id = {message.MessageId} , body - {message.Body}");
            context.Logger.Log($"** message attributes keys  {string.Join(", ", message.MessageAttributes.Keys)}");
            context.Logger.Log($"** message attributes' values  {string.Join(", ", message?.MessageAttributes?.Values.Select(v=>v.StringValue))} ");

            try
            {
                //check which task has finished execution                 
                var completedWithinTime = Task.WaitAll(new []{Task.Run(() => this.CallUnpredictableExternalAPI(message, context.Logger))}, TimeSpan.FromSeconds(TimeOutSeconds));

                if (completedWithinTime)//happy path
                {
                   context.Logger.Log($"**** happy path, message {message.Body} was resent to another queue ******");
                }
                else//External call has timed out, so have to increase visibility timeout
                {
                    context.Logger.Log($"**** external call thread timed out ******");
                    context.Logger.Log($"**** seconds left in lambda {context.RemainingTime.Seconds} ******");
                    await this.ResendMessageToAnotherQueue(InputQueueUrl, message, context.Logger, DelayMillisecondsSeconds);
                }
            }
            //we are here if external call threw exception
            catch (AggregateException e)
            {
                context.Logger.Log($"**** exception details {e} ******");
                context.Logger.Log($"**** seconds left in lambda {context.RemainingTime.Seconds} ******");
                await this.ResendMessageToAnotherQueue(InputQueueUrl, message, context.Logger, DelayMillisecondsSeconds);
            }
            context.Logger.Log("____________________ processing complete ____________________");
        }


        private async Task<SendMessageResponse> ResendMessageToAnotherQueue(string queue, SQSEvent.SQSMessage message, ILambdaLogger logger, int delaySeconds = 0)
        {
            logger.Log($" ** sending message {message.Body} to  queue {queue} ** ");
            var writeMessageRequest = new SendMessageRequest(queue, message.Body);

            int retryCounter = 0;
            if (message.MessageAttributes.ContainsKey(RetryCount))
            {
                retryCounter = Convert.ToInt32(message.MessageAttributes[RetryCount].StringValue);
                retryCounter++;
            }

            writeMessageRequest.MessageAttributes = new Dictionary<string, MessageAttributeValue>();
            writeMessageRequest.MessageAttributes.Add(RetryCount, new MessageAttributeValue() { DataType = "String", StringValue = (retryCounter).ToString() });

            //Normalize distribution of incoming messages in time by function x2
            writeMessageRequest.DelaySeconds = retryCounter * retryCounter * delaySeconds;

            return await this.SqSClient.SendMessageAsync(writeMessageRequest);
        }

        /// <summary>
        /// This method fails or times out with at least 20% chance
        /// </summary>
        private Task CallUnpredictableExternalAPI(SQSEvent.SQSMessage message, ILambdaLogger contextLogger)
        {
            contextLogger.Log(" ** inside CallExternalService **");

            //about 10% of requests should explode
            var seed = Guid.NewGuid().GetHashCode();
            contextLogger.Log($" ** checking RND seed {seed} **");
            var rnd = new Random(seed);
            int failureChance = rnd.Next(1, 11);
            if (failureChance == 5)
            {
                contextLogger.Log($" ** about to throw exception, failureChance = {failureChance} **");
                throw new ApplicationException();
            }
          
            //about 10% of requests should time out
            failureChance = rnd.Next(1, 11);
            if (failureChance == 10)
            {
                contextLogger.Log($" ** about to freeze,  failureChance = {failureChance} **");
                Thread.Sleep(TimeOutSeconds*1000);
                return Task.Run(()=>{});
            }

            //this is happy path
            return this.ResendMessageToAnotherQueue(OutputQueueUrl,message, contextLogger);
        }

        private async Task ChangeVisibilityTimeOutAsyncTask(SQSEvent.SQSMessage message, ILambdaLogger contextLogger, int timeoutSeconds)
        {
            contextLogger.Log($" *** about to change visibility timeout  to {timeoutSeconds}  seconds***");

            var batRequest = new ChangeMessageVisibilityRequest
            {
                ReceiptHandle = message.ReceiptHandle,
                VisibilityTimeout = timeoutSeconds,
                QueueUrl = InputQueueUrl
            };

            var response = await this.SqSClient.ChangeMessageVisibilityAsync(batRequest);
            contextLogger.Log($" *** visibility timeout result status code is {response.HttpStatusCode} ***");

        }
       
    }
}
