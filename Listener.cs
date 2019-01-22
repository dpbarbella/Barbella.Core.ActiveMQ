using Apache.NMS;
using Apache.NMS.ActiveMQ;
using System;

namespace Barbella.Utils.ActiveMQ
{
    public sealed class Listener
    {
        public delegate void MapMessageReceived(IMapMessage mapMessage);
        public event MapMessageReceived OnMapMessageReceived;
        public delegate void TextMessageReceived(ITextMessage textMessage);
        public event TextMessageReceived OnTextMessageReceived;

        private IConnectionFactory _connectionFactory;
        private IConnection _connection;
        private ISession _session;

        private readonly string _topic;

        public Listener(string uri, string topic)
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

        public void Start()
        {
            IDestination destination;
            IMessage message;
            ITextMessage textMessage;
            IMapMessage mapMessage;

            try
            {
                destination = _session.GetTopic(_topic);

                using (IMessageConsumer consumer = _session.CreateConsumer(destination))
                {
                    while (true)
                    {
                        message = consumer.Receive();

                        if (message != null)
                        {
                            mapMessage = message as IMapMessage;

                            if (mapMessage != null)
                            {
                                OnMapMessageReceived?.Invoke(mapMessage);
                            }
                            else
                            {
                                textMessage = message as ITextMessage;

                                if (textMessage != null)
                                {
                                    OnTextMessageReceived?.Invoke(textMessage);
                                }
                                else
                                {
                                    //NEEDS WORK HERE...
                                }
                            }
                        }
                    }
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
                message = null;
                textMessage = null;
            }
        }
    }
}
