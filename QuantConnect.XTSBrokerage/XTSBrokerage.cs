/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using System;
using System.Linq;
using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Data.Market;
using QuantConnect.Interfaces;
using QuantConnect.Logging;
using QuantConnect.Orders;
using QuantConnect.Orders.Fees;
using QuantConnect.Securities;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Threading;
using Quobject.SocketIoClientDotNet.Client;
using Newtonsoft.Json;
using System.Globalization;
using XTSAPI.Interactive;
using XTSAPI.MarketData;
using QuantConnect.XTSBrokerage;
using QuantConnect.Api;
using QuantConnect.Util;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Packets;
using XTSAPI;

namespace QuantConnect.Brokerages.XTS
{
    [BrokerageFactory(typeof(XTSBrokerageFactory))]
    public class XtsBrokerage : Brokerage, IDataQueueHandler
    {
        private IDataAggregator _aggregator;
        //private const int ConnectionTimeout = 30000;
        private IAlgorithm _algorithm;
        private readonly CancellationTokenSource _ctsFillMonitor = new CancellationTokenSource();
        private readonly AutoResetEvent _fillMonitorResetEvent = new AutoResetEvent(false);
        private Task _fillMonitorTask;
        private readonly int _fillMonitorTimeout = Config.GetInt("XTS.FillMonitorTimeout", 500);
        private readonly ConcurrentDictionary<int, decimal> _fills = new ConcurrentDictionary<int, decimal>();
        private readonly ConcurrentDictionary<string, Order> _pendingOrders = new ConcurrentDictionary<string, Order>();
        private string _interactiveApiKey;
        private string _interactiveApiSecret;
        private BrokerageConcurrentMessageHandler<Socket> _messageHandler;

        private string _marketApiKey;
        private string _marketApiSecret;
        private readonly MarketHoursDatabase _mhdb = MarketHoursDatabase.FromDataFolder();
        private readonly object _connectionLock = new();
        protected Socket interactiveSocket;
        protected Socket marketDataSocket;
        private XTSInteractive interactive = null;
        private XTSMarketData marketdata = null;
        private string interactiveToken;
        private List<Task> _checkConnectionTask;
        private readonly List<long> _subscribeInstrumentTokens = new List<long>();
        private XTSSymbolMapper _XTSMapper;
        private string userID;
        // MIS/CNC/NRML
        private string _XTSProductType;
        private DataQueueHandlerSubscriptionManager _subscriptionManager;
        private ISecurityProvider _securityProvider;
        private readonly ConcurrentDictionary<long, Symbol> _subscriptionsById = new ConcurrentDictionary<long, Symbol>();

        //EQUITY / COMMODITY
        //private string _tradingSegment;
        private bool _isInitialized;
        private bool interactiveSocketConnected = false;
        private bool marketDataSocketConnected = false;
        private bool socketConnected = false;

        /// <summary>
        /// Returns true if we're currently connected to the broker
        /// </summary>
        public override bool IsConnected => socketConnected;



        /// <summary>
        /// A list of currently active orders
        /// </summary>
        public ConcurrentDictionary<int, Order> CachedOrderIDs = new ConcurrentDictionary<int, Order>();
        private string marketDataToken;



        /// <summary>
        /// Parameterless constructor for brokerage
        /// </summary>
        /// <remarks>This parameterless constructor is required for brokerages implementing <see cref="IDataQueueHandler"/></remarks>
        public XtsBrokerage()
            : this(Composer.Instance.GetPart<IDataAggregator>())
        {
        }


        /// <summary>
        /// Constructor for brokerage
        /// </summary>
        /// <param name="tradingSegment">Trading Segment</param>
        /// <param name="productType">Product Type</param>
        /// <param name="apiKey">api key</param>
        /// <param name="apiSecret">api secret</param>
        /// <param name="algorithm">the algorithm instance is required to retrieve account type</param>
        /// <param name="yob">year of birth</param>
        public XtsBrokerage(string tradingSegment, string productType, string interactiveSecretKey,
            string interactiveapiKey, string marketSecretKey, string marketApiKey, IAlgorithm algorithm, IDataAggregator aggregator)
            : base("XTS")
        {
            Initialize(tradingSegment, productType, interactiveSecretKey, interactiveapiKey, marketSecretKey, marketApiKey, algorithm, aggregator);
        }

        /// <summary>
        /// Creates a new instance
        /// </summary>
        /// <param name="aggregator">consolidate ticks</param>
        public XtsBrokerage(IDataAggregator aggregator) : base("XTSBrokerage")
        {
            // Useful for some brokerages:

            // Brokerage helper class to lock websocket message stream while executing an action, for example placing an order
            // avoid race condition with placing an order and getting filled events before finished placing
        }

        #region IDataQueueHandler

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _aggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Brokerage

        /// <summary>
        /// Gets all open orders on the account.
        /// NOTE: The order objects returned do not have QC order IDs.
        /// </summary>
        /// <returns>The open orders returned from IB</returns>
        public override List<Order> GetOpenOrders()
        {
            var allOrders = interactive.GetOrderAsync();

            List<Order> list = new List<Order>();

            //Only loop if there are any actual orders inside response
            if (allOrders.IsCompleted && allOrders.Result.Length > 0)
            {

                foreach (var item in allOrders.Result.Where(z => z.OrderStatus.ToUpperInvariant() == "OPEN" || z.OrderStatus.ToUpperInvariant() == "NEW"))
                {
                    Order order;
                    var contract = XTSInstrumentList.GetContractInfoFromInstrumentID(item.ExchangeInstrumentID);
                    var brokerageSecurityType = _XTSMapper.GetBrokerageSecurityType(item.ExchangeInstrumentID);
                    var symbol = _XTSMapper.GetLeanSymbol(contract.Name, brokerageSecurityType, Market.India);
                    var time = Convert.ToDateTime(item.OrderGeneratedDateTime, CultureInfo.InvariantCulture);
                    var price = Convert.ToDecimal(item.OrderPrice, CultureInfo.InvariantCulture);
                    var quantity = item.LeavesQuantity;

                    if (item.OrderType.ToUpperInvariant() == "MARKET")
                    {
                        order = new MarketOrder(symbol, quantity, time);
                    }
                    else if (item.OrderType.ToUpperInvariant() == "LIMIT")
                    {
                        order = new LimitOrder(symbol, quantity, price, time);
                    }
                    else if (item.OrderType.ToUpperInvariant() == "STOPMARKET")
                    {
                        order = new StopMarketOrder(symbol, quantity, price, time);
                    }
                    else if (item.OrderType.ToUpperInvariant() == "STOPLIMIT")
                    {
                        order = new StopLimitOrder();
                    }
                    else
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, item.MessageCode,
                            "XTSBrorage.GetOpenOrders: Unsupported order type returned from brokerage: " + item.OrderType));
                        continue;
                    }

                    order.BrokerId.Add(item.ExchangeOrderID);
                    order.Status = ConvertOrderStatus(item);

                    list.Add(order);
                }
                foreach (var item in list)
                {
                    if (item.Status.IsOpen())
                    {
                        var cached = CachedOrderIDs.Where(c => c.Value.BrokerId.Contains(item.BrokerId.First()));
                        if (cached.Any())
                        {
                            CachedOrderIDs[cached.First().Key] = item;
                        }
                    }
                }
            }
            return list;
        }


        private OrderStatus ConvertOrderStatus(OrderResult orderDetails)
        {
            var filledQty = Convert.ToInt32(orderDetails.OrderQuantity, CultureInfo.InvariantCulture);
            var pendingQty = Convert.ToInt32(orderDetails.LeavesQuantity, CultureInfo.InvariantCulture);
            var orderDetail = interactive.GetOrderAsync(orderDetails.AppOrderID);
            if (filledQty == 0)
            {
                return OrderStatus.Submitted;
            }
            else if (filledQty > 0 && pendingQty > 0 && orderDetails.OrderStatus.ToUpperInvariant() == "PARTIALLYFILLED")
            {
                return OrderStatus.PartiallyFilled;
            }
            else if (pendingQty == 0 && orderDetails.OrderStatus.ToUpperInvariant() == "FILLED")
            {
                return OrderStatus.Filled;
            }
            else if (orderDetail.Result[orderDetail.Result.Length-1].OrderStatus.ToUpperInvariant() == "CANCELLED")
            {
                return OrderStatus.Canceled;
            }

            return OrderStatus.None;
        }

        /// <summary>
        /// Gets all holdings for the account
        /// </summary>
        /// <returns>The current holdings from the account</returns>
        public override List<Holding> GetAccountHoldings()
        {
            var holdingsList = new List<Holding>();
            var xtsProductTypeUpper = _XTSProductType.ToUpperInvariant();
            var productTypeMIS = "MIS";
            var productTypeCNC = "CNC";
            var productTypeNRML = "NRML";
            // get MIS and NRML Positions
            if (string.IsNullOrEmpty(xtsProductTypeUpper) || xtsProductTypeUpper == productTypeMIS)
            {
                var positions = interactive.GetDayPositionAsync();  //position include child positions...
                if (positions.Result.positionList.Length > 0)
                {
                    foreach (var position in positions.Result.positionList)
                    {
                        //We only need Intraday positions here, Not carryforward postions
                        if (position.ProductType.ToUpperInvariant() == productTypeMIS)
                        {
                            Holding holding = new Holding
                            {
                                AveragePrice = Convert.ToDecimal((position.NetAmount.ToDecimal() / position.Quantity.ToDecimal()), CultureInfo.InvariantCulture),
                                Symbol = _XTSMapper.GetLeanSymbol(position.TradingSymbol, _XTSMapper.GetBrokerageSecurityType(position.ExchangeInstrumentID.ToInt64()), Market.India),
                                MarketPrice = Convert.ToDecimal(position.BuyAveragePrice, CultureInfo.InvariantCulture),
                                Quantity = position.Quantity.ToDecimal(),
                                UnrealizedPnL = Convert.ToDecimal(position.UnrealizedMTM, CultureInfo.InvariantCulture),
                                CurrencySymbol = Currencies.GetCurrencySymbol("INR"),
                                MarketValue = Convert.ToDecimal(position.BuyAmount)
                            };
                            holdingsList.Add(holding);
                        }
                    }
                }
            }
            // get CNC Positions
            if (string.IsNullOrEmpty(xtsProductTypeUpper) || xtsProductTypeUpper == productTypeCNC)
            {
                var holdingResponse = interactive.GetHoldingsAsync(userID);
                if (holdingResponse.IsCompleted && holdingResponse.Result.RMSHoldingList != null)
                {
                    //var symbol = marketdata.SearchByIdAsync();
                    foreach (var item in holdingResponse.Result.RMSHoldingList)
                    {
                        Holding holding = new Holding
                        {
                            AveragePrice = item.IsBuyAvgPriceProvided ? item.BuyAvgPrice : 0,
                            //Symbol = _XTSMapper.GetLeanSymbol(item.TargetProduct, _XTSMapper.GetBrokerageSecurityType(interactive)),
                            MarketPrice = 0,
                            Quantity = Convert.ToDecimal(item.HoldingQuantity, CultureInfo.InvariantCulture),
                            //UnrealizedPnL = (item.averagePrice - item.lastTradedPrice) * item.holdingsQuantity,
                            CurrencySymbol = Currencies.GetCurrencySymbol("INR"),
                            //MarketValue = item.lastTradedPrice * item.holdingsQuantity
                        };
                        holdingsList.Add(holding);
                    }
                }
            }
            // get NRML Positions
            if (string.IsNullOrEmpty(xtsProductTypeUpper) || xtsProductTypeUpper == productTypeNRML)
            {
                var positions = interactive.GetNetPositionAsync();
                if (positions.IsCompleted)
                {
                    foreach (var position in positions.Result.positionList)
                    {
                        //We only need carry forward NRML positions here, Not intraday postions.
                        if (position.ProductType.ToUpperInvariant() == productTypeNRML)
                        {
                            Holding holding = new Holding
                            {
                                AveragePrice = Convert.ToDecimal((position.NetAmount.ToDecimal() / position.Quantity.ToDecimal()), CultureInfo.InvariantCulture),
                                Symbol = _XTSMapper.GetLeanSymbol(position.TradingSymbol, _XTSMapper.GetBrokerageSecurityType(position.ExchangeInstrumentID.ToInt64()), Market.India),
                                MarketPrice = Convert.ToDecimal(position.BuyAveragePrice, CultureInfo.InvariantCulture),
                                Quantity = position.Quantity.ToDecimal(),
                                UnrealizedPnL = Convert.ToDecimal(position.UnrealizedMTM, CultureInfo.InvariantCulture),
                                CurrencySymbol = Currencies.GetCurrencySymbol("INR"),
                                MarketValue = Convert.ToDecimal(position.BuyAmount)
                            };
                            holdingsList.Add(holding);
                        }
                    }
                }
            }
            return holdingsList;

        }

        /// <summary>
        /// Gets the current cash balance for each currency held in the brokerage account
        /// </summary>
        /// <returns>The current cash balance for each currency available for trading</returns>
        public override List<CashAmount> GetCashBalance()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Places a new order and assigns a new broker ID to the order
        /// </summary>
        /// <param name="order">The order to be placed</param>
        /// <returns>True if the request for a new order has been placed, false otherwise</returns>
        public override bool PlaceOrder(Order order)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Updates the order with the same id
        /// </summary>
        /// <param name="order">The new order information</param>
        /// <returns>True if the request was made for the order to be updated, false otherwise</returns>
        public override bool UpdateOrder(Order order)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Cancels the order with the specified ID
        /// </summary>
        /// <param name="order">The order to cancel</param>
        /// <returns>True if the request was made for the order to be canceled, false otherwise</returns>
        public override bool CancelOrder(Order order)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Connects the client to the broker's remote servers
        /// </summary>
        public override void Connect()
        {
            lock (_connectionLock)
            {
                if (IsConnected)
                    return;
                
                //To connect with Order related Broadcast

                if (!interactiveSocketConnected)
                {
                    Log.Trace("XTSBrokerage.Connect(): Connecting...");
                    InteractiveLoginResult login;
                    Task<InteractiveLoginResult> login1 = interactive.LoginAsync<InteractiveLoginResult>(_interactiveApiKey, _interactiveApiSecret, "WebAPI");
                    login1.Wait();
                    if (login1.IsCompleted && login1 != null)
                    {
                        if (interactive.ConnectToSocket())
                        {
                            interactiveSocketConnected = true;
                            if (_checkConnectionTask[0] == null)
                            {
                                // we start a task that will be in charge of expiring and refreshing our session id
                                _checkConnectionTask[0] = Task.Factory.StartNew(CheckConnection, _ctsFillMonitor.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                            }
                        }
                        else
                        {
                            throw new ArgumentException("Server not connected: Check UserId or Token or server url ");
                        }
                    }
                }

                //To connect with MarketData Broadcast
                if (!marketDataSocketConnected)
                {
                    Log.Trace("XTSMarketData.Connect(): Connecting...");
                    MarketDataLoginResult mlogin;
                    Task<MarketDataLoginResult> mlogin1 = marketdata.LoginAsync<MarketDataLoginResult>(_marketApiKey, _marketApiSecret, "WebAPI");
                    mlogin1.Wait();
                    if (mlogin1.IsCompleted && mlogin1 != null)
                    {
                        MarketDataPorts[] marketports = {
                                                    MarketDataPorts.marketDepthEvent,MarketDataPorts.candleDataEvent,MarketDataPorts.exchangeTradingStatusEvent,
                                                    MarketDataPorts.generalMessageBroadcastEvent,MarketDataPorts.indexDataEvent
                                                 };
                        if (marketdata.ConnectToSocket(marketports, PublishFormat.JSON, BroadcastMode.Full, "WebAPI"))
                        {
                            marketDataSocketConnected = true;
                            if (_checkConnectionTask[1] == null)
                            {
                                // we start a task that will be in charge of expiring and refreshing our session id
                                _checkConnectionTask[1] = Task.Factory.StartNew(CheckConnection, _ctsFillMonitor.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
                            }
                        }
                    }
                }
                if(marketDataSocketConnected && interactiveSocketConnected) { socketConnected= true; }

            }
        }


        private void CheckConnection()
        {
            var timeoutLoop = TimeSpan.FromMinutes(1);
            while (!_ctsFillMonitor.Token.IsCancellationRequested)
            {
                _ctsFillMonitor.Token.WaitHandle.WaitOne(timeoutLoop);

                try
                {
                    // we start trying to reconnect during extended market hours so we are all set for normal hours
                    if (!IsConnected && IsExchangeOpen(extendedMarketHours: true))
                    {
                        marketDataSocketConnected = false;
                        interactiveSocketConnected= false;
                        socketConnected= false;
                        Log.Trace($"XTSBrokerage.CheckConnection(): resetting connection...",
                            overrideMessageFloodProtection: true);

                        try
                        {
                            Disconnect();
                        }
                        catch
                        {
                            // don't let it stop us from reconnecting
                        }
                        Thread.Sleep(100);

                        // create a new instance
                        Connect();
                    }
                }
                catch (Exception e)
                {
                    Log.Error(e);
                }
            }
        }



        private bool IsExchangeOpen(bool extendedMarketHours)
        {
            var leanSymbol = Symbol.Create("SBIN", SecurityType.Equity, Market.India);
            var securityExchangeHours = _mhdb.GetExchangeHours(Market.India, leanSymbol, SecurityType.Equity);
            var localTime = DateTime.UtcNow.ConvertFromUtc(securityExchangeHours.TimeZone);
            return securityExchangeHours.IsOpen(localTime, extendedMarketHours);
        }



        /// <summary>
        /// Disconnects the client from the broker's remote servers
        /// </summary>
        public override void Disconnect()
        {
            if(IsConnected)
            {
                interactiveSocket.Close();
            }
        }

        #endregion

        #region IDataQueueUniverseProvider

        /// <summary>
        /// Method returns a collection of Symbols that are available at the data source.
        /// </summary>
        /// <param name="symbol">Symbol to lookup</param>
        /// <param name="includeExpired">Include expired contracts</param>
        /// <param name="securityCurrency">Expected security currency(if any)</param>
        /// <returns>Enumerable of Symbols, that are associated with the provided Symbol</returns>
        public IEnumerable<Symbol> LookupSymbols(Symbol symbol, bool includeExpired, string securityCurrency = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns whether selection can take place or not.
        /// </summary>
        /// <remarks>This is useful to avoid a selection taking place during invalid times, for example IB reset times or when not connected,
        /// because if allowed selection would fail since IB isn't running and would kill the algorithm</remarks>
        /// <returns>True if selection can take place</returns>
        public bool CanPerformSelection()
        {
            throw new NotImplementedException();
        }

        #endregion

        private static bool CanSubscribe(Symbol symbol)
        {
            var market = symbol.ID.Market;
            var securityType = symbol.ID.SecurityType;
            if (symbol.Value.IndexOfInvariant("universe", true) != -1 || symbol.IsCanonical())
            {
                return false;
            }
            // Include future options as a special case with no matching market, otherwise our
            // subscriptions are removed without any sort of notice.
            return
                (securityType == SecurityType.Equity ||
                securityType == SecurityType.Option ||
                securityType == SecurityType.Index ||
                securityType == SecurityType.Future) &&
                market == Market.India;
        }


        /// <summary>
        /// Subscribes to the requested symbols (using an individual streaming channel)
        /// </summary>
        /// <param name="symbols">The list of symbols to subscribe</param>
        public void Subscribe(IEnumerable<Symbol> symbols)
        {
            if (symbols.Count() <= 0)
            {
                return;
            }
            var sub = new SubscriptionPayload();
            ContractInfo contract;
            //re add already subscribed symbols and send in one go
            foreach (var instrumentID in _subscribeInstrumentTokens)
            {
                try
                {
                    contract = XTSInstrumentList.GetContractInfoFromInstrumentID(instrumentID);
                    List<Instruments> instruments = new List<Instruments> { new Instruments { exchangeSegment = (int)marketdata.GetExchangeSegmentFromString(contract.ExchangeSegment), exchangeInstrumentID = contract.ExchangeInstrumentID } };
                    Task<QuoteResult<ListQuotesBase>> data = marketdata.SubscribeAsync<ListQuotesBase>(1502,instruments);
                    data.Wait();
                    if(data.Result != null)
                    {

                        Log.Trace($"InstrumentID: {instrumentID} subscribed");
                    }
                }
                catch (Exception exception)
                {
                    throw new Exception($"XTSBrokerage.Subscribe(): Message: {exception.Message} Exception: {exception.InnerException}");
                }
            }
            foreach (var symbol in symbols)
            {
                try
                {
                    var contractString = _XTSMapper.GetBrokerageSymbol(symbol);
                    contract = JsonConvert.DeserializeObject<ContractInfo>(contractString);
                    var instrumentID = contract.ExchangeInstrumentID;
                    if (!_subscribeInstrumentTokens.Contains(instrumentID))
                    {
                        contract = XTSInstrumentList.GetContractInfoFromInstrumentID(instrumentID);
                        List<Instruments> instruments = new List<Instruments> { new Instruments { exchangeSegment = (int)marketdata.GetExchangeSegmentFromString(contract.ExchangeSegment), exchangeInstrumentID = contract.ExchangeInstrumentID } };
                        Task<QuoteResult<ListQuotesBase>> data = marketdata.SubscribeAsync<ListQuotesBase>(1502, instruments);
                        data.Wait();
                        if (data.Result != null)
                        {

                            Log.Trace($"InstrumentID: {instrumentID} subscribed");
                        }
                        _subscribeInstrumentTokens.Add(instrumentID);
                        _subscriptionsById[instrumentID] = symbol;
                    }
                }
                catch (Exception exception)
                {
                    throw new Exception($"XTSBrokerage.Subscribe(): Message: {exception.Message} Exception: {exception.InnerException}");
                }
            }
            
        }

        /// <summary>
        /// Removes the specified symbols to the subscription
        /// </summary>
        /// <param name="symbols">The symbols to be removed keyed by SecurityType</param>
        private bool Unsubscribe(IEnumerable<Symbol> symbols)
        {
            throw new NotImplementedException();
        }



        private void Initialize(string tradingSegment, string productType, string interactiveSecretKey,
            string interactiveapiKey, string marketSecretKey, string marketeapiKey, IAlgorithm algorithm, IDataAggregator aggregator)
        {
            if (_isInitialized)
            {
                return;
            }
            _isInitialized = true;
            //_tradingSegment = tradingSegment;
            _XTSProductType = productType;
            _algorithm = algorithm;
            _securityProvider = algorithm?.Portfolio;
            _aggregator = aggregator;
            interactive = new XTSInteractive(Config.Get("xts-url") + "/interactive" );
            marketdata = new XTSMarketData(Config.Get("xts-url") + "/marketdata");
            _interactiveApiKey = interactiveapiKey;
            _interactiveApiSecret = interactiveSecretKey;
            //_messageHandler = new BrokerageConcurrentMessageHandler<Socket>(OnMessageImpl);
            _marketApiSecret= marketSecretKey;
            _marketApiKey = marketeapiKey;

            var subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();
            //TODO:IMPLEMENT For Marketdata Socket

            interactiveSocket = IO.Socket(Config.Get("xts-url").ToString(), new IO.Options { ForceNew = true });
            SubscribeToConnectionEvent("interactive");
            interactiveSocket.On(Socket.EVENT_MESSAGE, (data) =>
            {

            });
            interactiveSocket.On(Socket.EVENT_DISCONNECT, (data) => {
                Log.Trace($"XTSBrokerage(): Socket Diconnected.");
                interactiveSocketConnected = false;
                socketConnected= false;
            });
            interactiveSocket.On(Socket.EVENT_CONNECT, (data) => {
                Log.Trace($"XTSBrokerage(): Socket.Open. Subscribing");
                //Subscribe((IEnumerable<Symbol>)GetSubscribed());
            });

            subscriptionManager.SubscribeImpl += (s, t) =>
            {
                Subscribe(s);
                return true;
            };
            subscriptionManager.UnsubscribeImpl += (s, t) => Unsubscribe(s);

            _subscriptionManager = subscriptionManager;
            _fillMonitorTask = Task.Factory.StartNew(FillMonitorAction, _ctsFillMonitor.Token);

            //ValidateSubscription();
            Log.Trace("XTSBrokerage(): Start XTS Brokerage");
        }

        private void SubscribeToConnectionEvent(string type)
        {
            Socket socket = null;

            if (type == "interactive")
                socket = interactiveSocket;
            else
                socket = marketDataSocket;

            if (socket == null)
                return;


            socket.On(Socket.EVENT_CONNECT, (data) =>
            {
                
            });

            socket.On("joined", (data) =>
            {
                
            });

            socket.On("success", (data) =>
            {
                
            });

            socket.On("warning", (data) =>
            {
                
            });

            socket.On("error", (data) =>
            {
                
            });

            socket.On("logout", (data) =>
            {
               
            });

            socket.On("disconnect", (data) =>
            {
                Log.Trace($"XTSBrokerage(): Socket Diconnected.");
                interactiveSocketConnected = false;
                socketConnected = false;
            });
        }

        private IEnumerable<Symbol> GetSubscribed()
        {
            //throw new NotImplementedException();
            return _subscriptionManager.GetSubscribedSymbols() ?? Enumerable.Empty<Symbol>();
        }

        private void OnMessageImpl(WebSocketMessage webSocketMessage)
        {
           
        }

        private void WaitTillConnected()
        {
            while (!IsConnected)
            {
                Thread.Sleep(500);
            }
        }

        private void FillMonitorAction()
        {
            Log.Trace("XTSBrokerage.FillMonitorAction(): task started");

            try
            {
                WaitTillConnected();
                foreach (var order in GetOpenOrders())
                {
                    _pendingOrders.TryAdd(order.BrokerId.First(), order);
                }

                while (!_ctsFillMonitor.IsCancellationRequested)
                {
                    try
                    {
                        WaitTillConnected();
                        _fillMonitorResetEvent.WaitOne(TimeSpan.FromMilliseconds(_fillMonitorTimeout), _ctsFillMonitor.Token);

                        foreach (var kvp in _pendingOrders)
                        {
                            var orderId = kvp.Key;
                            var order = kvp.Value;

                            var response = interactive.GetOrderAsync(orderId.ToInt32()); //Need to implemented
                            var result = response.Result;
                            if (response.Status != null)
                            {
                                if (response.Status.ToLower() == "failure")
                                {
                                    OnMessage(new BrokerageMessageEvent(
                                        BrokerageMessageType.Warning,
                                        -1,
                                        $"XTSBrokerage.FillMonitorAction(): request failed: [{response.Status}] {result[0].MessageCode}, Content: {response.ToString()}, ErrorMessage: {result}"));

                                    continue;
                                }
                            }

                            //Process cancelled orders here.
                            if (result[0].OrderStatus.ToLower() == "cancel")
                            {
                                OnOrderClose(result[0]);
                            }

                            if (result[0].OrderStatus.ToLower() == "executed")
                            {
                                // Process rest of the orders here.
                               // EmitFillOrder(result[0]);
                            }
                        }
                    }
                    catch (Exception exception)
                    {
                        OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Warning, -1, exception.Message));
                    }
                }
            }
            catch (Exception exception)
            {
                OnMessage(new BrokerageMessageEvent(BrokerageMessageType.Error, -1, exception.Message));
            }

            Log.Trace("XTSBrokerage.FillMonitorAction(): task ended");
        }


        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;
            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }



        private void OnOrderClose(OrderResult orderDetails)  //XTS order details 
        {
            var brokerId = orderDetails.OrderReferenceID;
            if (orderDetails.OrderStatus.ToLower() == "cancel")
            {
                var order = CachedOrderIDs
                    .FirstOrDefault(o => o.Value.BrokerId.Contains(brokerId))
                    .Value;
                if (order == null)
                {
                    order = _algorithm.Transactions.GetOrdersByBrokerageId(brokerId)?.SingleOrDefault();
                    if (order == null)
                    {
                        // not our order, nothing else to do here
                        return;
                    }
                }
                Order outOrder;
                if (CachedOrderIDs.TryRemove(order.Id, out outOrder))
                {
                    OnOrderEvent(new OrderEvent(order,
                        DateTime.UtcNow,
                        OrderFee.Zero,
                        "XTS Order Event")
                    { Status = OrderStatus.Canceled });
                }
            }
        }



        private void EmitFillOrder(OrderIdResult orderResponse) 
        {
            throw new NotImplementedException();
        }
    }
}
