/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencord.kafka.integrations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.onosproject.net.device.DeviceService;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opencord.igmpproxy.IgmpStatisticsEvent;
import org.opencord.igmpproxy.IgmpStatisticsEventListener;
import org.opencord.igmpproxy.IgmpStatisticsService;

/**
 * Listens for IGMP events and pushes them on a Kafka bus.
 */

@Component(immediate = true)
public class IgmpKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL_UNARY,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindIgmpStatService",
            unbind = "unbindIgmpStatService")
    protected IgmpStatisticsService igmpStatisticsService;

    private final IgmpStatisticsEventListener igmpStatisticsEventListener =
            new InternalIgmpStatisticsListner();

    //TOPIC
    private static final String IGMP_STATISTICS_TOPIC = "onos.igmp.stats.kpis";

   // IGMP stats event params
    private static final String IGMP_JOIN_REQ = "igmpJoinReq";
    private static final String IGMP_SUCCESS_JOIN_REJOIN_REQ = "igmpSuccessJoinRejoinReq";
    private static final String IGMP_FAIL_JOIN_REQ = "igmpFailJoinReq";
    private static final String IGMP_LEAVE_REQ = "igmpLeaveReq";
    private static final String IGMP_DISCONNECT = "igmpDisconnect";
    private static final String IGMP_V3_MEMBERSHIP_QUERY = "igmpv3MembershipQuery";
    private static final String IGMP_V1_MEMBERSHIP_REPORT = "igmpv1MemershipReport";
    private static final String IGMP_V2_MEMBERSHIP_REPORT = "igmpv2MemershipReport";
    private static final String IGMP_V3_MEMBERSHIP_REPORT = "igmpv3MemershipReport";
    private static final String IGMP_V2_LEAVE_GROUP = "igmpv2LeaveGroup";
    private static final String TOTAL_MSG_RECEIVED = "totalMsgReceived";
    private static final String IGMP_MSG_RECEIVED = "igmpMsgReceived";
    private static final String INVALID_IGMP_MSG_RECEIVED = "invalidIgmpMsgReceived";

    protected void bindIgmpStatService(IgmpStatisticsService igmpStatisticsService) {
        log.info("bindIgmpStatService");
        if (this.igmpStatisticsService == null) {
            log.info("Binding IgmpStastService");
            this.igmpStatisticsService = igmpStatisticsService;
            log.info("Adding listener on IgmpStatService");
            igmpStatisticsService.addListener(igmpStatisticsEventListener);
        } else {
            log.warn("Trying to bind IgmpStatService but it is already bound");
        }
    }

    protected void unbindIgmpStatService(IgmpStatisticsService igmpStatisticsService) {
        log.info("unbindIgmpStatService");
        if (this.igmpStatisticsService == igmpStatisticsService) {
            log.info("Unbinding IgmpStatService");
            this.igmpStatisticsService = null;
            log.info("Removing listener on IgmpStatService");
            igmpStatisticsService.removeListener(igmpStatisticsEventListener);
        } else {
            log.warn("Trying to unbind IgmpStatService but it is already unbound");
        }
    }

    @Activate
    public void activate() {
        log.info("Started IgmpKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped IgmpKafkaIntegration");
    }

    private void handleStat(IgmpStatisticsEvent event) {
        eventBusService.send(IGMP_STATISTICS_TOPIC, serializeStat(event));
        log.info("IGMPStatisticsEvent sent successfully");
    }

    private JsonNode serializeStat(IgmpStatisticsEvent event) {
        log.info("Serializing IgmpStatisticsEvent");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode igmpStatEvent = mapper.createObjectNode();
        igmpStatEvent.put(IGMP_JOIN_REQ, event.subject().getIgmpJoinReq());
        igmpStatEvent.put(IGMP_SUCCESS_JOIN_REJOIN_REQ, event.subject().getIgmpSuccessJoinRejoinReq());
        igmpStatEvent.put(IGMP_FAIL_JOIN_REQ, event.subject().getIgmpFailJoinReq());
        igmpStatEvent.put(IGMP_LEAVE_REQ, event.subject().getIgmpLeaveReq());
        igmpStatEvent.put(IGMP_DISCONNECT, event.subject().getIgmpDisconnect());
        igmpStatEvent.put(IGMP_V3_MEMBERSHIP_QUERY, event.subject().getIgmpv3MembershipQuery());
        igmpStatEvent.put(IGMP_V1_MEMBERSHIP_REPORT, event.subject().getIgmpv1MemershipReport());
        igmpStatEvent.put(IGMP_V2_MEMBERSHIP_REPORT, event.subject().getIgmpv2MembershipReport());
        igmpStatEvent.put(IGMP_V3_MEMBERSHIP_REPORT, event.subject().getIgmpv3MembershipReport());
        igmpStatEvent.put(IGMP_V2_LEAVE_GROUP, event.subject().getIgmpv2LeaveGroup());
        igmpStatEvent.put(TOTAL_MSG_RECEIVED, event.subject().getTotalMsgReceived());
        igmpStatEvent.put(IGMP_MSG_RECEIVED, event.subject().getIgmpMsgReceived());
        igmpStatEvent.put(INVALID_IGMP_MSG_RECEIVED, event.subject().getInvalidIgmpMsgReceived());
        return igmpStatEvent;
    }

    public class InternalIgmpStatisticsListner implements
                              IgmpStatisticsEventListener {

        @Override
        public void event(IgmpStatisticsEvent igmpStatisticsEvent) {
            handleStat(igmpStatisticsEvent);
        }
    }
}
