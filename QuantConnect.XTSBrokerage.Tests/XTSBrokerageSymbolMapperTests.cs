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
using System.ComponentModel;
using XTSAPI.MarketData;

namespace QuantConnect.Tests.Brokerages.XTS
{
    [TestFixture]
    public class XTSSymbolMapperTests
    {

        [Test]
        public void createLeansymbol(/*string brokerage, SecurityType securitytype, DateTime date, decimal strike, OptionRight right*/)
        {
            var data = XTSInstrumentList.Instance();
            var contract = XTSInstrumentList.GetContractInfoFromInstrumentID(57805);
            var sym = XTSInstrumentList.CreateLeanSymbol(contract);
            var mapper = new XTSSymbolMapper();
            var BrokerageSymbol = mapper.GetBrokerageSymbol(sym);
            var info = JsonConvert.DeserializeObject<ContractInfo>(BrokerageSymbol);
            mapper.GetLeanSymbol(BrokerageSymbol, SecurityType.Index, Market.India, info.ContractExpiration, info.StrikePrice.ToString().ToDecimal(),OptionRight.Call);

        }
    }
}