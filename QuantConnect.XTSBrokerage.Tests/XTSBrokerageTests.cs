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

using NUnit.Framework;
using QuantConnect.Tests;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Tests.Brokerages;
using Moq;
using QuantConnect.Brokerages;
using QuantConnect.Configuration;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Tests.Common.Securities;
using System;
using Deedle;
using QuantConnect.Data;
using QuantConnect.Brokerages.XTS;
using Newtonsoft.Json;
using QuantConnect.XTSBrokerage;
using XTSAPI.MarketData;
using System.Collections.Generic;

namespace QuantConnect.XTSBrokerages.Tests
{
    [TestFixture]
    public partial class XTSBrokerageTests : BrokerageTests
    {
        protected override Symbol Symbol => Symbols.SBIN;
        /// <summary>
        /// Gets the security type associated with the <see cref="BrokerageTests.Symbol"/>
        /// </summary>
        protected override SecurityType SecurityType => SecurityType.Equity;
        List<Symbol> _symbols = new List<Symbol>();
        long[] instrumnts = { 26000, 26001, 26002, 26003 } ;
        protected override IBrokerage CreateBrokerage(IOrderProvider orderProvider, ISecurityProvider securityProvider)
        {
            var securities = new SecurityManager(new TimeKeeper(DateTime.UtcNow, TimeZones.Kolkata))
            {
                { Symbol, CreateSecurity(Symbol) }
            };

            var transactions = new SecurityTransactionManager(null, securities);
            transactions.SetOrderProcessor(new FakeOrderProcessor());

            var algorithm = new Mock<IAlgorithm>();
            algorithm.Setup(a => a.Transactions).Returns(transactions);
            algorithm.Setup(a => a.BrokerageModel).Returns(new XTSBrokerageModel());
            algorithm.Setup(a => a.Portfolio).Returns(new SecurityPortfolioManager(securities, transactions));
            var interactiveSecretKey = Config.Get("xts-interactive-secretkey");
            var interactiveapiKey = Config.Get("xts-interactive-appkey");
            var marketApiKey = Config.Get("xts-marketdata-appkey");
            var marketSecretKey = Config.Get("xts-marketdata-secretkey");
            var yob = Config.Get("samco-year-of-birth");
            var tradingSegment = Config.Get("xts-trading-segment");
            var productType = Config.Get("xts-product-type");
            var xts = new XtsBrokerage(tradingSegment, productType, interactiveSecretKey,
            interactiveapiKey, marketSecretKey, marketApiKey, algorithm.Object, new AggregationManager());
            var data = XTSInstrumentList.Instance();
            foreach (var instrument in instrumnts)
            {
                var contract = XTSInstrumentList.GetContractInfoFromInstrumentID(instrument);
                var sym = XTSInstrumentList.CreateLeanSymbol(contract);
                var mapper = new XTSSymbolMapper();
                var BrokerageSymbol = mapper.GetBrokerageSymbol(sym);
                var info = JsonConvert.DeserializeObject<ContractInfo>(BrokerageSymbol);
                var security = mapper.GetBrokerageSecurityType(info.ExchangeInstrumentID);
                var symbol = mapper.GetLeanSymbol(info.Name, security, Market.India, info.ContractExpiration, info.StrikePrice.ToString().ToDecimal(), OptionRight.Call);
                _symbols.Add(symbol);
            }
            xts.Subscribe(_symbols);
            Console.WriteLine(_symbols.Count);
            return xts;
        }


        protected override bool IsAsync()
        {
            throw new System.NotImplementedException();
        }

        protected override decimal GetAskPrice(Symbol symbol)
        {
            throw new System.NotImplementedException();
        }


        /// <summary>
        /// Provides the data required to test each order type in various cases
        /// </summary>
        private static TestCaseData[] OrderParameters()
        {
            return new[]
            {
                new TestCaseData(new MarketOrderTestParameters(Symbols.BTCUSD)).SetName("MarketOrder"),
                new TestCaseData(new LimitOrderTestParameters(Symbols.BTCUSD, 10000m, 0.01m)).SetName("LimitOrder"),
                new TestCaseData(new StopMarketOrderTestParameters(Symbols.BTCUSD, 10000m, 0.01m)).SetName("StopMarketOrder"),
                new TestCaseData(new StopLimitOrderTestParameters(Symbols.BTCUSD, 10000m, 0.01m)).SetName("StopLimitOrder"),
                new TestCaseData(new LimitIfTouchedOrderTestParameters(Symbols.BTCUSD, 10000m, 0.01m)).SetName("LimitIfTouchedOrder")
            };
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void CancelOrders(OrderTestParameters parameters)
        {
            base.CancelOrders(parameters);
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void LongFromZero(OrderTestParameters parameters)
        {
            base.LongFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void CloseFromLong(OrderTestParameters parameters)
        {
            base.CloseFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void ShortFromZero(OrderTestParameters parameters)
        {
            base.ShortFromZero(parameters);
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void CloseFromShort(OrderTestParameters parameters)
        {
            base.CloseFromShort(parameters);
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void ShortFromLong(OrderTestParameters parameters)
        {
            base.ShortFromLong(parameters);
        }

        [Test, TestCaseSource(nameof(OrderParameters))]
        public override void LongFromShort(OrderTestParameters parameters)
        {
            base.LongFromShort(parameters);
        }
    }
}