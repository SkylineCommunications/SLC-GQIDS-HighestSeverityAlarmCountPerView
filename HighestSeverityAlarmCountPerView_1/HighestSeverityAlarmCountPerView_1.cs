/*
****************************************************************************
*  Copyright (c) 2023,  Skyline Communications NV  All Rights Reserved.    *
****************************************************************************

By using this script, you expressly agree with the usage terms and
conditions set out below.
This script and all related materials are protected by copyrights and
other intellectual property rights that exclusively belong
to Skyline Communications.

A user license granted for this script is strictly for personal use only.
This script may not be used in any way by anyone without the prior
written consent of Skyline Communications. Any sublicensing of this
script is forbidden.

Any modifications to this script by the user are only allowed for
personal use and within the intended purpose of the script,
and will remain the sole responsibility of the user.
Skyline Communications will not be responsible for any damages or
malfunctions whatsoever of the script resulting from a modification
or adaptation by the user.

The content of this script is confidential information.
The user hereby agrees to keep this confidential information strictly
secret and confidential and not to disclose or reveal it, in whole
or in part, directly or indirectly to any person, entity, organization
or administration without the prior written consent of
Skyline Communications.

Any inquiries can be addressed to:

	Skyline Communications NV
	Ambachtenstraat 33
	B-8870 Izegem
	Belgium
	Tel.	: +32 51 31 35 69
	Fax.	: +32 51 31 01 29
	E-mail	: info@skyline.be
	Web		: www.skyline.be
	Contact	: Ben Vandenberghe

****************************************************************************
Revision History:

DATE		VERSION		AUTHOR			COMMENTS

19/10/2023	1.0.0.1		Sebastiaan, Skyline	Initial version
****************************************************************************
*/

namespace HighestSeverityAlarmCountPerView_1
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Skyline.DataMiner.Analytics.GenericInterface;
    using Skyline.DataMiner.Net.Filters;
    using Skyline.DataMiner.Net.Helper;
    using Skyline.DataMiner.Net.Messages;

    [GQIMetaData(Name = "Highest severity alarm count per view")]
    public class ActiveAlarmsFilter : IGQIDataSource, IGQIOnInit, IGQIOnPrepareFetch
    {
        private static readonly GQIIntColumn IDColumn = new GQIIntColumn("View ID");
        private static readonly GQIStringColumn SeverityColumn = new GQIStringColumn("Severity");
        private static readonly GQIIntColumn CountColumn = new GQIIntColumn("Count");

        private GQIDMS _dms;
        private Task<List<Tuple<int, string, int>>> _viewToSeverityCounts;

        public OnInitOutputArgs OnInit(OnInitInputArgs args)
        {
            _dms = args.DMS;
            return new OnInitOutputArgs();
        }

        public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
        {
            _viewToSeverityCounts = Task.Factory.StartNew(() =>
            {
                return GetViewCounts();
            });
            return new OnPrepareFetchOutputArgs();
        }

        public GQIColumn[] GetColumns()
        {
            return new GQIColumn[]
            {
                IDColumn,
                SeverityColumn,
                CountColumn,
            };
        }

        public GQIPage GetNextPage(GetNextPageInputArgs args)
        {
            if (_viewToSeverityCounts == null)
                return new GQIPage(new GQIRow[0]);

            _viewToSeverityCounts.Wait();

            var viewToSeverityCounts = _viewToSeverityCounts.Result;
            if (viewToSeverityCounts == null)
                throw new GenIfException("No alarms found.");

            if (viewToSeverityCounts.Count == 0)
                return new GQIPage(new GQIRow[0]);

            var rows = new List<GQIRow>(viewToSeverityCounts.Count);

            foreach (var viewToSeverityCount in viewToSeverityCounts)
            {
                var cells = new[]
                {

                    new GQICell {Value= viewToSeverityCount.Item1 }, // ID Column
                    new GQICell {Value= viewToSeverityCount.Item2 }, // Severity Column
                    new GQICell {Value= viewToSeverityCount.Item3 }, // Count Column
                };

                rows.Add(new GQIRow(cells));
            }

            return new GQIPage(rows.ToArray()) { HasNextPage = false };
        }

        private List<Tuple<int, string, int>> GetViewCounts()
        {
            var viewToAlarms = TryMapViewToSeverityToAlarms();
            if (viewToAlarms == null)
                return null;

            return TransformViewAlarms(viewToAlarms);
        }

        private Dictionary<int, Dictionary<string, List<AlarmEventMessage>>> TryMapViewToSeverityToAlarms()
        {
            var viewToAlarms = new Dictionary<int, Dictionary<string, List<AlarmEventMessage>>>();

            if (!TryFillWithActiveAlarms(viewToAlarms))
                return null;

            if (!TryFillWithViews(viewToAlarms))
                return null;

            return viewToAlarms;
        }

        private bool TryFillWithActiveAlarms(Dictionary<int, Dictionary<string, List<AlarmEventMessage>>> viewToAlarms)
        {
            var msg = new GetActiveAlarmsMessage();
            var alarmsResponse = _dms.SendMessage(msg) as ActiveAlarmsResponseMessage;
            if (alarmsResponse?.ActiveAlarms == null)
                return false;

            var alarms = alarmsResponse.ActiveAlarms.WhereNotNull().ToList();
            foreach (var alarm in alarms.WhereNotNull())
            {
                var viewImpacts = alarm.ViewImpactInfo?.WhereNotNull().ToList();
                if (viewImpacts == null || viewImpacts.Count == 0)
                    continue;

                foreach (var viewInfo in viewImpacts)
                {
                    if (!viewToAlarms.TryGetValue(viewInfo.ViewID, out var severityMap))
                    {
                        severityMap = new Dictionary<string, List<AlarmEventMessage>>(StringComparer.OrdinalIgnoreCase);
                        viewToAlarms[viewInfo.ViewID] = severityMap;
                    }

                    if (!severityMap.TryGetValue(alarm.Severity, out var severityList))
                    {
                        severityList = new List<AlarmEventMessage>();
                        severityMap[alarm.Severity] = severityList;
                    }

                    severityList.Add(alarm);
                }
            }

            return true;
        }

        private bool TryFillWithViews(Dictionary<int, Dictionary<string, List<AlarmEventMessage>>> viewToAlarms)
        {
            var msg = new GetInfoMessage(InfoType.ViewInfo);
            var views = _dms.SendMessages(msg).OfType<ViewInfoEventMessage>().ToArray();

            if (views == null)
                return false;

            foreach (var viewInfo in views.WhereNotNull())
            {
                if (viewToAlarms.ContainsKey(viewInfo.ID))
                    continue;

                viewToAlarms[viewInfo.ID] = new Dictionary<string, List<AlarmEventMessage>> { { "Normal", new List<AlarmEventMessage>(0) } };
            }

            return true;
        }

        private List<Tuple<int, string, int>> TransformViewAlarms(Dictionary<int, Dictionary<string, List<AlarmEventMessage>>> viewAlarms)
        {
            return viewAlarms.Select(x =>
            {
                List<AlarmEventMessage> alarms = null;
                if (x.Value.TryGetValue("Critical", out alarms))
                    return new Tuple<int, string, int>(x.Key, "Critical", alarms.Count);
                if (x.Value.TryGetValue("Major", out alarms))
                    return new Tuple<int, string, int>(x.Key, "Major", alarms.Count);
                if (x.Value.TryGetValue("Minor", out alarms))
                    return new Tuple<int, string, int>(x.Key, "Minor", alarms.Count);
                if (x.Value.TryGetValue("Warning", out alarms))
                    return new Tuple<int, string, int>(x.Key, "Warning", alarms.Count);
                if (x.Value.TryGetValue("Normal", out alarms))
                    return new Tuple<int, string, int>(x.Key, "Normal", alarms.Count);

                return null;
            }).WhereNotNull().ToList();
        }
    }
}