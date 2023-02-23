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

using QuantConnect.Configuration;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Packets;
using QuantConnect.Securities;
using QuantConnect.Util;
using System;
using System.Collections.Generic;

namespace QuantConnect.Brokerages.XTS
{
    /// <summary>
    /// Provides a XTS implementation of BrokerageFactory
    /// </summary>
    public class XTSBrokerageFactory : BrokerageFactory
    {
        /// <summary>
        /// Gets the brokerage data required to run the brokerage from configuration/disk
        /// </summary>
        /// <remarks>
        /// The implementation of this property will create the brokerage data dictionary required for
        /// running live jobs. See <see cref="IJobQueueHandler.NextJob"/>
        /// </remarks>
        public override Dictionary<string, string> BrokerageData => new Dictionary<string, string>
        {
            { "xts-interactive-appkey", Config.Get("xts-interactive-appkey") },
            { "xts-interactive-secretkey", Config.Get("xts-interactive-secretkey") },
            { "xts-marketdata-appkey", Config.Get("xts-marketdata-appkey") },
            { "xts-marketdata-secretkey", Config.Get("xts-marketdata-secretkey") },
            { "xts-trading-segment" ,Config.Get("xts-trading-segment") },
            { "xts-product-type", Config.Get("xts-product-type") }
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="XTSBrokerageFactory"/> class
        /// </summary>
        public XTSBrokerageFactory() : base(typeof(XtsBrokerage))
        {
        }

        /// <summary>
        /// Gets a brokerage model that can be used to model this brokerage's unique behaviors
        /// </summary>
        /// <param name="orderProvider">The order provider</param>
        public override IBrokerageModel GetBrokerageModel(IOrderProvider orderProvider)/* => new XTSBrokerageModel();*/
        {
            throw new NotImplementedException();
        }


        /// <summary>
        /// Creates a new IBrokerage instance
        /// </summary>
        /// <param name="job">The job packet to create the brokerage for</param>
        /// <param name="algorithm">The algorithm instance</param>
        /// <returns>A new brokerage instance</returns>
        public override IBrokerage CreateBrokerage(LiveNodePacket job, IAlgorithm algorithm)
        {
            var required = new[] { "xts-interactive-appkey", "xts-interactive-secretkey", "xts-marketdata-appkey", "xts-marketdata-secretkey", "xts-trading-segment", "xts-product-type" };

            foreach (var item in required)
            {
                if (string.IsNullOrEmpty(job.BrokerageData[item]))
                {
                    throw new Exception($"XRSFactory.CreateBrokerage: Missing {item} in config.json");
                }
            }
            var brokerage = new XtsBrokerage(
                job.BrokerageData["xts-trading-segment"],
                job.BrokerageData["xts-product-type"],
                job.BrokerageData["xts-interactive-secretkey"],
                job.BrokerageData["xts-interactive-appkey"],
                job.BrokerageData["xts-marketdata-secretkey"],
                job.BrokerageData["xts-marketdata-appkey"],
               
               algorithm,
               Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"),
                   forceTypeNameOnExisting: false));
            //Add the brokerage to the composer to ensure its accessible to the live data feed.
            Composer.Instance.AddPart<IDataQueueHandler>(brokerage);
            return brokerage;
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            
        }
    }
}