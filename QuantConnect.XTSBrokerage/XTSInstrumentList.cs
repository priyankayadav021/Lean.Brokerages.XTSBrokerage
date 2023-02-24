using CsvHelper.Configuration;
using CsvHelper;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using XTSAPI.MarketData;
using QuantConnect.Logging;
using QuantConnect.Configuration;
using QuantConnect.Util;
using System.Diagnostics.Contracts;
using XTSAPI;

namespace QuantConnect.XTSBrokerage
{
    public class XTSInstrumentList
    {
        private readonly object objectToLock = new object();
        private static readonly XTSInstrumentList instance = new XTSInstrumentList();
        private readonly TimeOnly updateReferenceTime = new TimeOnly(9, 00);
        private DateTime lastUpdateDateTime;
        public static List<ContractInfo> _XTSTradableContractList;
        //public List<Symbol> _leanSymbolList;
        private static Dictionary<ContractInfo, Symbol> _contractMapToLeanSymbol;
        private static Dictionary<long, ContractInfo> _instrumentIDToContractInfo;
        private static Dictionary<Symbol, ContractInfo> _leanSymbolToContractMap;
        protected string _getEndPoint = Config.Get("xts-url") + "/marketdata/instruments/master/";


        protected XTSInstrumentList()
        {
            _XTSTradableContractList = new List<ContractInfo>();
            //_leanSymbolList = new List<Symbol>();
            _contractMapToLeanSymbol= new Dictionary<ContractInfo, Symbol>();
            _leanSymbolToContractMap = new Dictionary<Symbol, ContractInfo>();
            _instrumentIDToContractInfo = new Dictionary<long, ContractInfo>();

            {
                updateData();
            }
        }

        public static XTSInstrumentList Instance()
        {
            return instance;
        }

        private void checkForUpdate()
        {
            DateOnly todayDate = DateOnly.FromDateTime(DateTime.Now);
            DateOnly updateDateOnly = DateOnly.FromDateTime(lastUpdateDateTime);
            TimeOnly updateTimeOnly = TimeOnly.FromDateTime(lastUpdateDateTime);
            if (!(todayDate == updateDateOnly && updateTimeOnly >= updateReferenceTime))
            {
                updateData();
            }
        }

     

        private void updateData()
        {
            lock (objectToLock)
            {
                _XTSTradableContractList?.Clear();
                //_leanSymbolList?.Clear();
                _instrumentIDToContractInfo?.Clear();

                var csvString = @"D:/Contract.csv";
                var streamReader = new StreamReader(csvString);

                //TextReader sr = new StringReader(csvString);
                //string[] HeaderEquity = { " ExchangeSegment ", " ExchangeInstrumentID ", " InstrumentType ", " Name ", " Description ",
                //" Series ", " NameWithSeries ", " InstrumentID ", " PriceBand.High ", " PriceBand.Low ", " FreezeQty ", " TickSize ", " LotSize ",
                //" Multiplier ", " displayName ", " ISIN ", " PriceNumerator ", " PriceDenominator "} ;

                //string[] Header = { " ExchangeSegment ", " ExchangeInstrumentID ", " InstrumentType ", " Name ", " Description ",
                //" Series ", " NameWithSeries ", " InstrumentID ", " PriceBand.High ", " PriceBand.Low ", " FreezeQty ", " TickSize ", " LotSize ",
                //" Multiplier ", " UnderlyingInstrumentId ", " UnderlyingIndexName " , " ContractExpiration ", " StrikePrice ", " OptionType", " displayName ", " ISIN ", " PriceNumerator ", " PriceDenominator "};

                CsvConfiguration configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = false,
                    MissingFieldFound = null
                };

                var csv = new CsvReader(streamReader, configuration);
                csv.Configuration.RegisterClassMap<ContractMapper>();
                //while (csv.Read())
                //{
                //    var contract = csv.GetRecord<ContractInfo>();
                //    Console.WriteLine(contract.InstrumentID);
                //}
                List<ContractInfo> contracts = csv.GetRecords<ContractInfo>().ToList();
                foreach (var contract in contracts)
                {
                    if ((contract.ExchangeSegment == (int)ExchangeSegment.NSECM || contract.ExchangeSegment == (int)ExchangeSegment.NSECO
                        || contract.ExchangeSegment == (int)ExchangeSegment.NSECD || contract.ExchangeSegment == (int)ExchangeSegment.NSEFO)
                        && (contract.Series == "EQ" || contract.Series == "FUTSTK" || contract.Series == "OPTSTK" ||
                        contract.Series == "INDEX" || contract.Series == "FUTIDX" || contract.Series == "OPTIDX"))
                    {
                        Symbol _sym = CreateLeanSymbol(contract);

                        _XTSTradableContractList.Add(contract);

                        _leanSymbolToContractMap[_sym] = contract;
                        _contractMapToLeanSymbol[contract] = _sym;
                        _instrumentIDToContractInfo[contract.ExchangeInstrumentID] = contract;

                    }
                }
                lastUpdateDateTime = DateTime.Now;
                streamReader.Close();
            }
        }


        /// <summary>
        /// Converts an XTS symbol to a Lean symbol instance
        /// </summary>
        /// <param name="contract">A Lean symbol instance</param>
        /// <returns>A new Lean Symbol instance</returns>
        public static Symbol CreateLeanSymbol(ContractInfo contract)
        {
            if (contract == null)
            {
                throw new ArgumentNullException(nameof(contract));
            }

            var securityType = SecurityType.Equity;
            switch (contract.Series.ToString())
            {
                // index
                case "INDEX":
                    {
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }

                //Index Options
                case "OPTIDX":
                    {
                        return createIndexOptionSymbol(contract);
                        break;
                    }
                //Index Futures
                case "FUTIDX":
                    {
                        return createIndexFuture(contract);
                        break;
                    }
                //Equities
                case "EQ":
                    {
                        return createEquitySymbol(contract);
                        break;
                    }
                //Stock Futures
                case "FUTSTK":
                    {
                        return createStkFutureSymbol(contract);
                        break;
                    }
                //Stock options
                case "OPTSTK":
                    {
                        return createEquityOptionSymbol(contract);
                        break;
                    }
                //Commodity Futures
                case "FUTCOM":
                    {
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Commodity Options
                case "OPTCOM":
                    {
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Bullion Options
                case "OPTBLN":
                    {
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Energy Futures
                case "FUTENR":
                    {
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Currenty Options
                case "OPTCUR":
                    {
                        //TODO: IMPLEMENT
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Currency Futures
                case "FUTCUR":
                    {
                        //TODO: IMPLEMENT
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Bond Futures
                case "FUTIRC":
                    {
                        //TODO:IMPLEMENT
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Bond Futures
                case "FUTIRT":
                    {
                        //TODO: IMPLEMENT
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
                //Bond Option
                case "OPTIRC":
                    {
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }

                default:
                    {
                        securityType = SecurityType.Base;
                        throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported");
                        break;
                    }
            }

           //return symbol;
        }

        public static Symbol createIndexFuture(ContractInfo contract)
        {
            DateTime _expiry;
            if (contract.Series != "FUTIDX")
            {
                throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported. This function is for FUTSTK");
            }
            //Symbol _index = Symbol.Create(contract.Name, SecurityType.Index, Market.India);
            //_expiry = DateTime.ParseExact(contract.ContractExpirationString, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            _expiry = contract.ContractExpiration;
            return Symbol.CreateFuture(contract.Name, Market.India, _expiry);
        }
        public static Symbol createStkFutureSymbol(ContractInfo contract)
        {
            DateTime _expiry;
            if (contract.Series != "FUTSTK")
            {
                throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {contract.Series} is not supported. This function is for FUTSTK");
            }
            //_expiry = DateTime.ParseExact(contract.ContractExpirationString, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            _expiry = contract.ContractExpiration;
            return Symbol.CreateFuture(contract.Name, Market.India, _expiry);
        }
        
        public static Symbol createEquitySymbol(ContractInfo eqScrip)
        {
            if (eqScrip.Series != "EQ")
            {
                throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {eqScrip.Series} is not supported. This function is for EQ");
            }
            string LeanTicker;
            if (eqScrip.NameWithSeries.EndsWith("-EQ")) { LeanTicker = eqScrip.NameWithSeries.Remove(eqScrip.NameWithSeries.Length - 3); } else { LeanTicker = eqScrip.NameWithSeries; }
            return Symbol.Create(LeanTicker, SecurityType.Equity, Market.India);
        }

        public static Symbol createIndexOptionSymbol(ContractInfo optionScrip)
        {
            OptionRight optionRight; decimal strikePrice; DateTime expiryDate; Symbol _underlying;
            if (optionScrip.Series != "OPTIDX")
            { throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} instrument type {optionScrip.Series} is not supported. This function is for OPTIDX"); }

            if (optionScrip.Description.EndsWithInvariant("PE", true))
            { optionRight = OptionRight.Put; }
            else if (optionScrip.Description.EndsWithInvariant("CE", true))
            { optionRight = OptionRight.Call; }
            else { throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} Invalid XTS script. Failed to determinine option right. TradingSymbol {optionScrip.Description} nither ends with CE nor PE"); }

            strikePrice = Convert.ToDecimal(optionScrip.StrikePrice, CultureInfo.InvariantCulture);
            //expiryDate = DateTime.ParseExact(optionScrip.ContractExpirationString, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            expiryDate = optionScrip.ContractExpiration;
            _underlying = Symbol.Create(optionScrip.Name, SecurityType.Index, Market.India);

            return Symbol.CreateOption(_underlying, Market.India, OptionStyle.European, optionRight, strikePrice, expiryDate);
        }


        public static Symbol createEquityOptionSymbol(ContractInfo optionScrip)
        {
            OptionRight optionRight; decimal strikePrice; DateTime expiryDate; Symbol _underlying;
            if (optionScrip.Series != "OPTSTK")
            { throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} Invalid XTS script. Function expects contract with instrument==OPTSTK {optionScrip}"); }

            if (optionScrip.Description.EndsWithInvariant("PE", true))
            { optionRight = OptionRight.Put; }
            else if (optionScrip.Description.EndsWithInvariant("CE", true))
            { optionRight = OptionRight.Call; }
            else { throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} Invalid XTS script. Failed to determinine option right. TradingSymbol {optionScrip.Description} nither ends with CE nor PE"); }

            strikePrice = Convert.ToDecimal(optionScrip.StrikePrice, CultureInfo.InvariantCulture);
            //expiryDate = DateTime.ParseExact(optionScrip.ContractExpirationString, "yyyy-MM-dd", CultureInfo.InvariantCulture);
            expiryDate = optionScrip.ContractExpiration;
            _underlying = Symbol.Create(optionScrip.Name, SecurityType.Equity, Market.India);
            return Symbol.CreateOption(_underlying, Market.India, OptionStyle.European, optionRight, strikePrice, expiryDate);
        }

        public static ContractInfo ConvertLeanSymbolToContractInfo(Symbol sym)
        {
           if(_leanSymbolToContractMap.TryGetValue(sym, out var contract)) {
                return contract; 
           }
            else
            {
                throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} Invalid Symbol. failed to find the symbol {sym}");
            }

        }


        public static Symbol ConvertContractInfoToLeanSymbol(ContractInfo contract)
        {
            Symbol sym;
            if (_contractMapToLeanSymbol.TryGetValue(contract, out sym))
            {
                return sym;
            }
            else
            {
                throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} Invalid Symbol. failed to find the symbol {sym}");
            }

        }



        public static ContractInfo GetContractInfoFromBrokerageSymbol(string brokerageSymbol, SecurityType securityType, DateTime expirationDate, decimal strike,OptionRight optionRight)
        {
            foreach(var contract in _XTSTradableContractList)
            {
                if (securityType == SecurityType.Equity && contract.Series == "EQ")
                {
                    if (contract.Name == brokerageSymbol && contract.ContractExpiration == expirationDate) return contract;
                }
                else if (securityType == SecurityType.Future && contract.Series == "FUTSTK")
                {
                    if (contract.Name == brokerageSymbol && contract.ContractExpiration == expirationDate) return contract;
                }
                else if (securityType == SecurityType.Option && contract.Series == "OPTSTK")
                {
                    if (contract.Name == brokerageSymbol && contract.ContractExpiration == expirationDate
                        && contract.OptionType == (int)optionRight + 3) return contract;
                }
                else if (securityType == SecurityType.Index && (contract.Series == "FUTIDX" || contract.Series == "OPTIDX" || contract.Series == "INDEX")) return contract;
            }
            return null;
        }

        public static ContractInfo GetContractInfoFromInstrumentID(long instrumentID)
        {
            
            if (_instrumentIDToContractInfo.TryGetValue(instrumentID, out var contract))
            {
                return contract;
            }
            else
            {
                throw new ArgumentException($"{WhoCalledMe.GetMethodName(1)} Invalid Symbol. failed to find the instrument {instrumentID}");
            }
        }


        public static ContractInfo GetListofInstrumentIDfromList(string name)
        {
            return _XTSTradableContractList.Where(s => s.Name == name.ToUpperInvariant()).SingleOrDefault();
        }

    }
}
