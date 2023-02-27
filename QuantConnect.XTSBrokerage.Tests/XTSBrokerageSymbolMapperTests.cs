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

using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Brokerages.XTS;
using QuantConnect.XTSBrokerage;
using RDotNet;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using XTSAPI.MarketData;

namespace QuantConnect.Tests.Brokerages.XTS
{
    [TestFixture]
    public class XTSSymbolMapperTests
    {
        List<Symbol> _symbols = new List<Symbol>();  

        [Test]
        [TestCase(57805)]
        [TestCase(10576)]//NIFTYBEES, NSECM
        [TestCase(57826)] //BANKNIFTY, NSEFO
        [TestCase(129004)] //HDFC, NSEFO
        public void createLeansymbol(/*string brokerage, SecurityType securitytype, DateTime date, decimal strike, OptionRight right*/long instrument)
        {
            var data = XTSInstrumentList.Instance();
            var contract = XTSInstrumentList.GetContractInfoFromInstrumentID(instrument);
            var sym = XTSInstrumentList.CreateLeanSymbol(contract);
            var mapper = new XTSSymbolMapper();
            var BrokerageSymbol = mapper.GetBrokerageSymbol(sym);
            var info = JsonConvert.DeserializeObject<ContractInfo>(BrokerageSymbol);
            var security = mapper.GetBrokerageSecurityType(info.ExchangeInstrumentID);
            var symbol = mapper.GetLeanSymbol(info.Name, security, Market.India, info.ContractExpiration, info.StrikePrice.ToString().ToDecimal(),OptionRight.Call);
            _symbols.Add(symbol);
            Subscribe();
        }

      
        public void Subscribe()
        {
            if (_symbols.Count > 3)
            {
                var xts = new XtsBrokerage();
                xts.Subscribe(_symbols);
            }

        }
    }
}