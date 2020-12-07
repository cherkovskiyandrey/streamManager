package com.tango.stream.manager.service.impl;

import com.google.common.math.Quantiles;
import com.tango.stream.manager.capacity.InitialViewersCapacityProvider;
import com.tango.stream.manager.dao.BroadcasterStatHistoryDao;
import com.tango.stream.manager.dao.LiveBroadcasterStatDao;
import com.tango.stream.manager.dao.LiveRegionDao;
import com.tango.stream.manager.dao.LiveStreamDao;
import com.tango.stream.manager.model.StreamKey;
import com.tango.stream.manager.model.ViewerCountStatsHistory;
import com.tango.stream.manager.model.ViewerCountStatsLive;
import com.tango.stream.manager.model.ViewersCapacity;
import com.tango.stream.manager.utils.EncryptUtil;
import org.apache.commons.configuration2.ImmutableConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.tango.stream.manager.capacity.InitialViewersCapacityStrategy.PLAIN_HISTORY;
import static com.tango.stream.manager.capacity.InitialViewersCapacityStrategy.STATIC;
import static com.tango.stream.manager.model.NodeType.EDGE;
import static com.tango.stream.manager.service.impl.BroadcasterStatServiceImpl.PERCENTILES_PROP;
import static com.tango.stream.manager.service.impl.BroadcasterStatServiceImpl.VIEWER_STRATEGY_PROP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BroadcasterStatServiceImplTest {
    public static final long ACC_ID = 42L;
    @Mock
    private LiveStreamDao liveStreamDao;
    @Mock
    private LiveBroadcasterStatDao liveBroadcasterStatDao;
    @Mock
    private LiveRegionDao liveRegionDao;
    @Mock
    private BroadcasterStatHistoryDao broadcasterStatHistoryDao;
    @Mock(lenient = true)
    private ImmutableConfiguration immutableConfiguration;
    @Mock
    private ConfigurationService configurationService;
    @Mock
    private InitialViewersCapacityProvider historyCapacityProvider;
    @Mock
    private InitialViewersCapacityProvider staticCapacityProvider;

    private BroadcasterStatServiceImpl statService;

    @BeforeEach
    void setUp() {
        when(historyCapacityProvider.getStrategy()).thenReturn(PLAIN_HISTORY);
        when(staticCapacityProvider.getStrategy()).thenReturn(STATIC);

        when(configurationService.get()).thenReturn(immutableConfiguration);
        when(immutableConfiguration.getString(VIEWER_STRATEGY_PROP)).thenReturn(PLAIN_HISTORY.getCode());

        statService = new BroadcasterStatServiceImpl(
                liveStreamDao,
                liveBroadcasterStatDao,
                liveRegionDao,
                broadcasterStatHistoryDao,
                configurationService,
                asList(historyCapacityProvider, staticCapacityProvider)
        );
    }

    @Test
    void shouldAddMissingRegionsFromStaticStrategy() {
        ViewersCapacity historyCapacity = ViewersCapacity.builder()
                .regionToViewers(new HashMap<String, Integer>() {{
                    put("a", 10);
                }})
                .build();
        when(historyCapacityProvider.getInitialViewersCapacity(anyLong())).thenReturn(historyCapacity);


        ViewersCapacity staticCapacity = ViewersCapacity.builder()
                .regionToViewers(new HashMap<String, Integer>() {{
                    put("a", -10);
                    put("b", 100);
                }})
                .build();
        when(staticCapacityProvider.getInitialViewersCapacity(anyLong())).thenReturn(staticCapacity);

        ViewersCapacity capacity = statService.getInitialViewersCapacity(ACC_ID);
        assertNotNull(capacity.getRegionToViewers());
        assertFalse(capacity.getRegionToViewers().isEmpty());
        assertEquals(10, capacity.getRegionToViewers().get("a"));
        assertEquals(100, capacity.getRegionToViewers().get("b"));
    }

    @Captor
    ArgumentCaptor<List<ViewerCountStatsHistory>> statsHistoryCaptor;

    @Test
    void shouldRestoreMissingObservations() {
        when(immutableConfiguration.getList(any(), eq(PERCENTILES_PROP), anyList())).thenReturn(asList(50, 80));

        Map<Long, Long> statsMap = new HashMap<Long, Long>() {{
            put(1L, 10L);
            put(2L, 20L);
            put(5L, 50L);
            put(7L, 70L);
        }};
        Map<Integer, Double> expectedPercentiles = Quantiles.percentiles()
                .indexes(50, 80)
                .compute(10, 20, 20, 20, 50, 50, 70);

        String region = "eu";
        when(liveRegionDao.getAllRegions(EDGE)).thenReturn(singletonList(region));
        when(liveBroadcasterStatDao.fetchStats(anyLong(), anyString())).thenReturn(Optional.of(ViewerCountStatsLive.builder()
                .accountId(ACC_ID)
                .statistics(statsMap)
                .build()));

        statService.onStreamTermination(EncryptUtil.encryptStreamKey(StreamKey.builder()
                .streamId(1L)
                .initTime(System.currentTimeMillis())
                .build()));

        verify(broadcasterStatHistoryDao).saveStats(statsHistoryCaptor.capture());
        List<ViewerCountStatsHistory> history = statsHistoryCaptor.getValue();
        for (ViewerCountStatsHistory historyEntry : history) {
            assertEquals(expectedPercentiles.get(historyEntry.getPercentile()).intValue(), historyEntry.getValue());
        }
    }
}