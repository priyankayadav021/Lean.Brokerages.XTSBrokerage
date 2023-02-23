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

using CsvHelper;
using CsvHelper.Configuration;
using Newtonsoft.Json;
using QuantConnect.Util;
using QuantConnect.XTSBrokerage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using XTSAPI.MarketData;

namespace QuantConnect.Brokerages.XTS
{
    /// <summary>
    /// Provides the mapping between Lean symbols and XTS symbols.
    /// </summary>
    public class XTSSymbolMapper : ISymbolMapper
    {

        /// <summary>
        /// Constructs default instance of the XTS Sybol Mapper
        /// </summary>
        public XTSSymbolMapper()
        {
            

        }

        public string GetBrokerageSymbol(Symbol symbol)
        {
            if (symbol == null || string.IsNullOrWhiteSpace(symbol.Value))
                throw new ArgumentException("XTSSymbolMapper.GetBrokerageSymbol(): Invalid symbol " + (symbol == null ? "null" : symbol.ToString()));
            
            ContractInfo contract = XTSInstrumentList.ConvertLeanSymbolToContractInfo(symbol);
            if(contract == null) { return null; }
            var contractString = JsonConvert.SerializeObject(contract);
            return contractString;
        }

        public Symbol GetLeanSymbol(string brokerageSymbol, SecurityType securityType, string market, DateTime expirationDate = default, decimal strike = 0, OptionRight optionRight = OptionRight.Call)
        {
             if (string.IsNullOrWhiteSpace(brokerageSymbol))
                throw new ArgumentException($"XTSSymbolMapper.GetLeanSymbol(): Invalid XTS symbol {brokerageSymbol}");

            if (securityType == SecurityType.Forex || securityType == SecurityType.Cfd || securityType == SecurityType.Commodity 
                || securityType == SecurityType.Crypto)
                throw new ArgumentException($"XTSSymbolMapper.GetLeanSymbol(): Unsupported security type {securityType}");

            if (!Market.Encode(market.ToLowerInvariant()).HasValue)
                throw new ArgumentException($"XTSSymbolMapper.GetLeanSymbol(): Invalid market {market}");
            ContractInfo contract = XTSInstrumentList.GetContractInfoFromBrokerageSymbol(brokerageSymbol,securityType,expirationDate,strike,optionRight);
            if (contract == null)
            {
                throw new ArgumentException($"XTSSymbolMapper.GetLeanSymbol(): Invalid XTS symbol {brokerageSymbol}");
            }
            return XTSInstrumentList.ConvertContractInfoToLeanSymbol(contract);
        }


        public SecurityType GetBrokerageSecurityType(long instrumentID)
        {
            ContractInfo contract = XTSInstrumentList.GetContractInfoFromInstrumentID(instrumentID);
            if (contract != null)
            {
                if(contract.Series == "FUTSTK") return SecurityType.Future;
                if(contract.Series == "OPTSTK") return SecurityType.Option;
                if (contract.Series == "EQ") return SecurityType.Equity;
                if(contract.Series == "INDEX" || contract.Series == "FUTIDX" || contract.Series == "OPTIDX") return SecurityType.Index;
            }
            return SecurityType.Base;
        }
    }
}