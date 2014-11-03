using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using Orient.Client.API;
using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol.Operations;
using Orient.Client.Protocol.Serializers;

namespace Orient.Client.Protocol
{
    public class Connection : IDisposable
    {
        //private readonly Logger _logger = LogManager.GetCurrentClassLogger();
        private TcpClient _socket;
        private BufferedStream _networkStream;
        private byte[] _readBuffer;
        private const int BufferLength = 1024;
        private readonly IServerAddress _serverAddress;
        private readonly IServerCredentials _serverCredentials;
        private readonly IDatabaseConnectionInfo _databaseConnectionInfo;
        private ConnectionType Type { get; set; }
        internal ODatabase Database { get; set; }
        internal bool IsReusable { get; private set; }
        private int SessionId { get; set; }
        public string DatabaseName {
            get { return _databaseConnectionInfo.DatabaseName; }
        }

        internal bool IsActive {
            get {
                // If the socket has been closed by your own actions (disposing the socket, 
                // calling methods to disconnect), Socket.Connected will return false. If 
                // the socket has been disconnected by other means, the property will return 
                // true until the next attempt to send or receive information.
                // more info: http://stackoverflow.com/questions/2661764/how-to-check-if-a-socket-is-connected-disconnected-in-c
                // why not to use socket.Poll solution: it fails when the socket is being initialized
                // and introduces additional delay for connection check
                return (_socket != null) && _socket.Connected;
            }
        }

        internal ODocument Document { get; private set; }

        internal Connection(IServerAddress serverAddress, IDatabaseConnectionInfo databaseConnectionInfo, bool isReusable) {
            if (serverAddress == null) throw new ArgumentNullException("serverAddress");
            if (databaseConnectionInfo == null) throw new ArgumentNullException("databaseConnectionInfo");

            _serverAddress = serverAddress;
            _databaseConnectionInfo = databaseConnectionInfo;
            Type = ConnectionType.Database;
            IsReusable = isReusable;
            ServerInfo.ProtocolVersion = 0;
            SessionId = -1;

            InitializeDatabaseConnection();
        }

        internal Connection(IServerAddress serverAddress, IServerCredentials serverCredentials) {
            if (serverAddress == null) throw new ArgumentNullException("serverAddress");
            if (serverCredentials == null) throw new ArgumentNullException("serverCredentials");

            _serverAddress = serverAddress;
            _serverCredentials = serverCredentials;
            _databaseConnectionInfo = null;
            Type = ConnectionType.Server;
            IsReusable = false;
            ServerInfo.ProtocolVersion = 0;
            SessionId = -1;

            InitializeServerConnection();
        }

        internal ODocument ExecuteOperation(IOperation operation) {
            var request = operation.Request(SessionId);

            foreach (var item in request.DataItems) {
                switch (item.Type) {
                    case "byte":
                    case "short":
                    case "int":
                    case "long":
                        Send(item.Data);
                        break;
                    case "record":
                        var buffer = new byte[2 + item.Data.Length];
                        Buffer.BlockCopy(BinarySerializer.ToArray(item.Data.Length), 0, buffer, 0, 2);
                        Buffer.BlockCopy(item.Data, 0, buffer, 2, item.Data.Length);
                        Send(buffer);
                        break;
                    case "bytes":
                    case "string":
                    case "strings":
                        //buffer = new byte[4 + item.Data.Length];
                        //Buffer.BlockCopy(BinarySerializer.ToArray(item.Data.Length), 0, buffer, 0, 4);
                        //Buffer.BlockCopy(item.Data, 0, buffer, 4, item.Data.Length);
                        //Send(buffer);

                        Send(BinarySerializer.ToArray(item.Data.Length));
                        Send(item.Data);
                        break;
                }
            }

            _networkStream.Flush();

            if (request.OperationMode != OperationMode.Synchronous) return null;
            try {
                var response = new Response(this);

                response.Receive();

                return operation.Response(response);
            } catch (Exception) {
                //reset connection as the socket may contain unread data and is considered unstable
                Reconnect();
                throw;
            }
        }

        private void Reconnect() {
            try {
                if (Type == ConnectionType.Database) {
                    var operation = new DbClose();
                    ExecuteOperation(operation);
                }
            } finally {
                Disconnect();

                if (Type == ConnectionType.Database) {
                    InitializeDatabaseConnection();
                } else {
                    InitializeServerConnection();
                }
            }
        }

        internal Stream GetNetworkStream() {
            return _networkStream;
        }


        public void Reload() {
            var dbReloadOperation = new DbReload();
            var document = ExecuteOperation(dbReloadOperation);
            Document.SetField("Clusters", document.GetField<List<OCluster>>("Clusters"));
            Document.SetField("ClusterCount", document.GetField<short>("ClusterCount"));
        }

        private void InitializeDatabaseConnection() {
            var dbOpenOperation = new DbOpen {
                DatabaseName = _databaseConnectionInfo.DatabaseName,
                DatabaseType = _databaseConnectionInfo.DatabaseType,
                UserName = _databaseConnectionInfo.Username,
                UserPassword = _databaseConnectionInfo.Password
            };

            InitializeConnection(dbOpenOperation);

        }

        private void InitializeServerConnection() {
            var connectionOperation = new Connect {
                UserName = _serverCredentials.RootUsername,
                UserPassword = _serverCredentials.RootPassword
            };
            InitializeConnection(connectionOperation);
        }

        private void InitializeConnection(IOperation connectionOperation) {
            _readBuffer = new byte[BufferLength];

            // initiate socket connection
            try {
                _socket = new TcpClient(_serverAddress.Hostname, _serverAddress.Port);
            } catch (SocketException ex) {
                throw new OException(OExceptionType.Connection, ex.Message, ex.InnerException);
            }

            _networkStream = new BufferedStream(_socket.GetStream());
            _networkStream.Read(_readBuffer, 0, 2);

            try {
                ServerInfo.ProtocolVersion = BinarySerializer.ToShort(_readBuffer.Take(2).ToArray());
            } catch (IOException e) {
                throw new OException(OExceptionType.Connection, "Cannot read protocol from server. " + e.Message,
                    e.InnerException);
            }


            if (ClientInfo.ProtocolVersion != ServerInfo.ProtocolVersion) {
                //_logger.Warn("Cliend and server protocol versions do not match. Client protocol is v{0} and Server is v{1}",ClientInfo.ProtocolVersion, ServerInfo.ProtocolVersion);
            }

            Document = ExecuteOperation(connectionOperation);
            SessionId = Document.GetField<int>("SessionId");
        }

        private void Send(byte[] rawData) {
            if ((_networkStream == null) || !_networkStream.CanWrite) return;

            try {
                _networkStream.Write(rawData, 0, rawData.Length);
            } catch (Exception ex) {
                //_logger.Error(ex);
                throw new OException(OExceptionType.Connection, ex.Message, ex.InnerException);
            }
        }

        private void Disconnect() {
            SessionId = -1;

            if ((_networkStream != null) && (_socket != null)) {
                _networkStream.Close();
                _socket.Close();
            }

            _networkStream = null;
            _socket = null;
        }

        public void Dispose() {
            Disconnect();
        }

    }
}