using Apache.NMS;
using Apache.NMS.ActiveMQ;
using System;
using System.Collections.Generic;

namespace Barbella.Utils.ActiveMQ
{
    public sealed class Producer
    {
        private IConnectionFactory _connectionFactory;
        private IConnection _connection;
        private ISession _session;

        private readonly string _topic;

        public Producer(string uri, string topic)
        {
            try
            {
                _topic = string.Concat(topic, "?consumer.dispatchAsync=false&consumer.prefetchSize=1&consumer.retroactive=true");

                _connectionFactory = new ConnectionFactory(uri);
                _connection = _connectionFactory.CreateConnection();
                _connection.Start();

                _session = _connection.CreateSession(AcknowledgementMode.AutoAcknowledge);
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                topic = null;
                uri = null;
            }
        }

        public void SendMessageMap(Dictionary<string, string> keyValuePairs)
        {
            IDestination destination;
            IMapMessage mapMessage;

            try
            {
                destination = _session.GetTopic(_topic);

                using (IMessageProducer producer = _session.CreateProducer(destination))
                {
                    mapMessage = producer.CreateMapMessage();

                    foreach (KeyValuePair<string, string> keyValuePair in keyValuePairs)
                    {
                        mapMessage.Body.SetString(keyValuePair.Key, keyValuePair.Value);
                    }

                    producer.Send(mapMessage);
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                destination = null;
                mapMessage = null;
            }
        }
    }
}
