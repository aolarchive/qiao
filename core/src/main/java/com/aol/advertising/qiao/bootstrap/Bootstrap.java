/****************************************************************************
 * Copyright (c) 2015 AOL Inc.
 * @author:     ytung05
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
 ****************************************************************************/

package com.aol.advertising.qiao.bootstrap;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.aol.advertising.qiao.agent.IAgent;
import com.aol.advertising.qiao.agent.IContainer;
import com.aol.advertising.qiao.agent.IFunnel;
import com.aol.advertising.qiao.agent.QiaoAgent;
import com.aol.advertising.qiao.agent.QiaoContainer;
import com.aol.advertising.qiao.config.ConfigConstants;
import com.aol.advertising.qiao.config.EmitterConfig;
import com.aol.advertising.qiao.config.InjectorConfig;
import com.aol.advertising.qiao.config.MultiSubnodeConfiguration;
import com.aol.advertising.qiao.config.PropertyValue;
import com.aol.advertising.qiao.config.QiaoConfig;
import com.aol.advertising.qiao.config.QiaoConfig.AgentConfig;
import com.aol.advertising.qiao.config.QiaoConfig.FunnelComponents;
import com.aol.advertising.qiao.config.AgentXmlConfiguration;
import com.aol.advertising.qiao.config.SingleSubnodeConfiguration;
import com.aol.advertising.qiao.emitter.IDataEmitter;
import com.aol.advertising.qiao.emitter.IDataEmitterContainer;
import com.aol.advertising.qiao.exception.ConfigurationException;
import com.aol.advertising.qiao.injector.IDataInjector;
import com.aol.advertising.qiao.injector.IInjectPositionCache;
import com.aol.advertising.qiao.injector.IInjectPositionCacheDependency;
import com.aol.advertising.qiao.management.IStatsCalculatorAware;
import com.aol.advertising.qiao.management.IStatsCollectable;
import com.aol.advertising.qiao.management.jmx.DynamicMBeanExporter;
import com.aol.advertising.qiao.management.metrics.IStatisticsStore;
import com.aol.advertising.qiao.management.metrics.StatsCalculator;
import com.aol.advertising.qiao.management.metrics.StatsCollector;
import com.aol.advertising.qiao.util.CommonUtils;
import com.aol.advertising.qiao.util.ContextUtils;
import com.aol.advertising.qiao.util.cache.PositionCache;

/**
 * Qiao's bootstrap process starts by loading qiao.xml and creates an agent
 * accordingly.
 */
public class Bootstrap implements IBootstrap, BeanPostProcessor
{
    private static final String funnelCfgXml = "classpath:qiao.xml";
    private static final String cfgProperties = "file:%s/local.properties";
    private static final String springContext = "applicationContext.xml";

    private static ApplicationContext appCtx;
    private static Bootstrap bootstrap;

    // -----------------------------------------
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private IContainer container;
    private AgentXmlConfiguration qiaoXmlConfig;

    private Map<String, Object> mbeanMap = new HashMap<String, Object>();
    private String mbeanDomain = "qiao";
    private DynamicMBeanExporter mbeanExporter;

    private StatsCollector statsCollector;
    private StatsCalculator statsCalculator;

    private String cacheName = "qiaoTempCache";
    private String cacheDir;

    private List<InjectorCachePair> injectorList = new ArrayList<InjectorCachePair>();


    /**
     * @param args
     */
    public static void main(String[] args)
    {
        registerShutdownHook();

        appCtx = new ClassPathXmlApplicationContext(springContext);
        try
        {
            bootstrap = ContextUtils.getBean(Bootstrap.class);
            bootstrap.init();
            bootstrap.start();
        }
        catch (Throwable e)
        {
            System.err.println("Failed to start:" + e.getMessage());
            e.printStackTrace();
        }
    }


    private static void registerShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                System.out.println(">> Shutdown hook ran!");
                if (bootstrap != null)
                    bootstrap.stop();
                System.out.println(">> shutdown completes");
            }
        });

    }


    private static AgentXmlConfiguration loadCofiguration(String funnelCfgXml,
            String cfgProperties)
    {
        AgentXmlConfiguration agent_config = new AgentXmlConfiguration();
        agent_config.setConfigXmlFileUri(funnelCfgXml);

        String cfg_dir = System.getProperty(ConfigConstants.PROP_QIAO_CFG_DIR);
        if (StringUtils.isBlank(cfg_dir))
        {
            System.err.println(
                    "System Property '" + ConfigConstants.PROP_QIAO_CFG_DIR
                            + "' has not been set. Process aborted.");
            System.exit(-1);
        }

        agent_config
                .setConfigPropertyFiles(String.format(cfgProperties, cfg_dir));

        agent_config.load();

        return agent_config;

    }


    @Override
    public void init() throws Exception
    {
        _validate();

        qiaoXmlConfig = loadCofiguration(funnelCfgXml, cfgProperties);

        statsCollector = getStatsCollector();
        statsCalculator = getStatsCalculator();

        container = composeAgent();
        container.init();

    }


    private void _validate()
    {
        if (mbeanExporter == null)
            throw new ConfigurationException("DynamicMBeanExporter not set");
    }


    private IContainer composeAgent() throws ClassNotFoundException
    {

        IContainer container = appCtx.getBean(QiaoContainer.class);

        resolveCacheDirectories();

        QiaoConfig qiao_cfg = qiaoXmlConfig.getQiaoConfig();
        Map<String, AgentConfig> agentCfgMap = qiao_cfg.getAgentConfigMap();
        MultiSubnodeConfiguration containernode_cfg = qiao_cfg
                .getAgentNodeConfig();

        List<IAgent> agent_list = new ArrayList<>();
        for (SingleSubnodeConfiguration agntnode_cfg : containernode_cfg)
        {
            String agent_id = agntnode_cfg.getId();
            IAgent agent = appCtx.getBean(QiaoAgent.class);
            agent.setId(agent_id);

            agent.setFunnels(
                    createFunnels(agentCfgMap.get(agent_id), agent_id));
            Map<String, PropertyValue> props = agntnode_cfg.getProperties();
            ContextUtils.injectMethods(agent, props);

            mbeanMap.put(mbeanDomain + ":type=QiaoAgent-" + agent_id, agent);

            agent_list.add(agent);

        }

        container.setAgents(agent_list);

        loadMBeanExporter();
        return container;
    }


    private void resolveCacheDirectories()
    {
        String sep = File.separator;

        String qiao_home = System.getProperty("qiao.home");
        if (qiao_home == null)
        {
            logger.info(
                    "System property 'qiao.home' not defined.  Set default cache dir to /tmp.");
            qiao_home = sep + "tmp";
        }

        String qiao_cache_dir = qiao_home + sep + "qiao_cache";

        this.cacheDir = qiao_cache_dir + sep + "curr";
    }


    private List<IFunnel> createFunnels(QiaoConfig.AgentConfig agentCfg,
            String agentId) throws ClassNotFoundException
    {
        List<IFunnel> flist = new ArrayList<IFunnel>();

        Map<String, String> id_map = agentCfg.getFunnelClassNames();

        Map<String, FunnelComponents> components = agentCfg
                .getFunnelComponents();

        MultiSubnodeConfiguration msub_cfg = agentCfg.getFunnelConfig();
        for (SingleSubnodeConfiguration sub : msub_cfg)
        {
            String funnel_id = sub.getId();
            String clzname = id_map.get(funnel_id);
            if (clzname == null)
                clzname = ConfigConstants.DEFAULT_FUNNEL_CLASSNAME;

            IFunnel funnel = this.<IFunnel> getBean(clzname);
            funnel.setId(funnel_id);
            ContextUtils.injectMethods(funnel, sub);

            String oname_pfx = mbeanDomain + ":type=QiaoAgent-" + agentId
                    + ",category=Funnel-" + funnel.getId();

            FunnelComponents fc = components.get(funnel_id);
            IDataInjector source = this
                    .createDataInjector(fc.getSourceConfig());
            source.setFunnelId(funnel_id);
            IDataEmitterContainer sink = this.createDataEmitter(funnel_id,
                    fc.getSinkConfig(), oname_pfx);
            sink.setFunnelId(funnel_id);

            funnel.setDataInjector(source);
            funnel.setDataEmitter(sink);

            String s = (String) sub
                    .getAttribute(ConfigConstants.CFGATTR_EMITTER_THREAD_COUNT); // thread count
            if (s != null)
                funnel.setEmitterThreadCount(Integer.parseInt(s));

            s = (String) sub.getAttribute(ConfigConstants.CFGATTR_QSIZE);
            if (s != null)
                funnel.setDataPipeCapacity(Integer.parseInt(s));

            s = (String) sub.getAttribute(ConfigConstants.CFGATTR_RATELIMIT);
            if (s != null)
                funnel.setRateLimit(Integer.parseInt(s));

            s = (String) sub.getAttribute(ConfigConstants.CFGATTR_AUTOSTART);
            if (s != null)
                funnel.setAutoStart(Boolean.parseBoolean(s));

            IStatisticsStore stats_store = getStatsStore(); // one per funnel
            funnel.setStatsStore(stats_store);

            flist.add(funnel);

            mbeanMap.put(oname_pfx, funnel);
            mbeanMap.put(oname_pfx + ",name=" + source.getId(), source);
            mbeanMap.put(oname_pfx + ",name=Statistics", stats_store);

        }

        for (InjectorCachePair pair : injectorList)
        {
            if (pair.initFromId != null
                    && (pair.injector instanceof IInjectPositionCacheDependency))
            {
                IInjectPositionCacheDependency to_cache = (IInjectPositionCacheDependency) pair.injector;

                Iterator<InjectorCachePair> iter = injectorList.iterator();
                while (iter.hasNext())
                {
                    InjectorCachePair from = iter.next();
                    if (from.injector.getId().equals(pair.initFromId))
                    {
                        to_cache.setPositionCacheDependency(from.positionCache);
                    }
                }
            }
        }
        return flist;
    }


    private IDataInjector createDataInjector(InjectorConfig srcCfg)
            throws ClassNotFoundException
    {
        IDataInjector src = this
                .<IDataInjector> getBean(srcCfg.getSourceClassName());
        src.setId(srcCfg.getId());

        // set instance-specific property values
        SingleSubnodeConfiguration sub_cfg = srcCfg.getSourceConfig();
        if (sub_cfg != null)
        {
            ContextUtils.injectMethods(src, sub_cfg);
        }

        // inject position cache if needed
        if (src instanceof IInjectPositionCache)
        {
            logger.info("Inject PositionCache to " + src.getClass());
            String suffix = null;
            if (src instanceof IDataInjector)
                suffix = ((IDataInjector) src).getId();

            PositionCache pos_cache = createPositionCache(suffix);
            IInjectPositionCache bean = (IInjectPositionCache) src;
            bean.setPositionCache(pos_cache);

            InjectorCachePair pair = new InjectorCachePair();
            pair.injector = src;
            pair.positionCache = bean.getPositionCache();

            String other_injector_id = (String) sub_cfg
                    .getAttribute(ConfigConstants.CFGATTR_INIT_POSITIONS_FROM);
            if (other_injector_id != null)
            {
                if (!(src instanceof IInjectPositionCacheDependency))
                    throw new ConfigurationException("injector " + src.getId()
                            + " does not implement IHasPositionCacheDependency while "
                            + ConfigConstants.CFGATTR_INIT_POSITIONS_FROM
                            + " specified");

                pair.initFromId = other_injector_id;
            }

            injectorList.add(pair);

        }

        return src;

    }


    private PositionCache createPositionCache(String id)
    {
        PositionCache position_cache = ContextUtils
                .getBean(PositionCache.class);
        String env_dir = (id == null ? cacheDir
                : cacheDir + File.separator + id);
        String dbname = (id == null ? cacheName : cacheName + id);
        String thread_name = (id == null ? "PositionCache"
                : "PositionCache_" + id);

        position_cache.setDbEnvDir(env_dir);
        position_cache.setDatabaseName(dbname);
        position_cache.setDataBinding();
        position_cache
                .setScheduler(CommonUtils.createScheduledThreadPoolExecutor(1,
                        CommonUtils.resolveThreadName(thread_name)));

        logger.info("PositionCache created => DbEnvDir=" + env_dir
                + ", DatabaseName=" + dbname);
        return position_cache;
    }


    private IDataEmitterContainer createDataEmitter(String funnelId,
            EmitterConfig emitterCfg, String jmxObjnamePrefix)
            throws ClassNotFoundException
    {
        IDataEmitterContainer container = this.<IDataEmitterContainer> getBean(
                emitterCfg.getEmitterContainerClassName());

        String oname_pfx = jmxObjnamePrefix;

        List<IDataEmitter> list = new ArrayList<IDataEmitter>();
        Map<String, IDataEmitter> id_map = new HashMap<String, IDataEmitter>();

        Map<String, String> map = emitterCfg.getEmitterClassNames(); // id -> classname

        MultiSubnodeConfiguration msub_cfg = emitterCfg.getSinkConfig();
        for (SingleSubnodeConfiguration sub : msub_cfg)
        {
            String id = sub.getId();
            String clzname = map.get(id);

            IDataEmitter sink = this.<IDataEmitter> getBean(clzname);
            sink.setId(id);
            sink.setFunnelId(funnelId);
            ContextUtils.injectMethods(sink, sub);

            list.add(sink);
            id_map.put(id, sink);

            mbeanMap.put(oname_pfx + ",comp=" + sink.getId(), sink);

        }

        container.setDataEmitterList(list);
        container.setIdEmitterMap(id_map);

        return container;
    }


    @Override
    public void start() throws Exception
    {
        logger.info("Starting agent...");
        container.start();
    }


    @Override
    public void stop()
    {
        if (container != null)
        {
            container.shutdown();
        }
    }


    @SuppressWarnings("unchecked")
    protected <A> A getBean(String className) throws ClassNotFoundException
    {
        logger.info("loading " + className + "...");
        Object o = null;
        try
        {
            o = ContextUtils.loadClass(className);
        }
        catch (NoSuchBeanDefinitionException e)
        {
            logger.info(e.getMessage());
        }

        if (o == null)
        {
            logger.info("Instantiate the class " + className
                    + " since no such spring bean defined");
            Class< ? > clz = Class.forName(className);
            try
            {
                o = clz.newInstance();
            }
            catch (Exception e)
            {
                throw new RuntimeException(
                        "Failed to instantiate class " + className);
            }
        }

        return (A) o;
    }


    private void loadMBeanExporter()
            throws BeansException, ClassNotFoundException
    {
        mbeanExporter.init();
        mbeanExporter.loadMBeans(mbeanMap);
    }


    private IStatisticsStore getStatsStore()
            throws BeansException, ClassNotFoundException
    {
        logger.info("loading bean "
                + ConfigConstants.DEFAULT_STAT_STORE_CLASSNAME + "...");
        return (IStatisticsStore) ContextUtils
                .loadClass(ConfigConstants.DEFAULT_STAT_STORE_CLASSNAME);

    }


    private StatsCalculator getStatsCalculator()
            throws BeansException, ClassNotFoundException
    {
        logger.info("loading bean "
                + ConfigConstants.DEFAULT_STAT_CALCULATOR_CLASSNAME + "...");
        return (StatsCalculator) ContextUtils
                .loadClass(ConfigConstants.DEFAULT_STAT_CALCULATOR_CLASSNAME);
    }


    private StatsCollector getStatsCollector()
            throws BeansException, ClassNotFoundException
    {
        logger.info("loading bean "
                + ConfigConstants.DEFAULT_STAT_COLLECTOR_CLASSNAME + "...");
        return (StatsCollector) ContextUtils
                .loadClass(ConfigConstants.DEFAULT_STAT_COLLECTOR_CLASSNAME);
    }


    public void setMbeanDomain(String mbeanDomain)
    {
        this.mbeanDomain = mbeanDomain;
    }


    public void setMbeanExporter(DynamicMBeanExporter mbeanExporter)
    {
        this.mbeanExporter = mbeanExporter;
    }


    public static ApplicationContext getAppCtx()
    {
        return appCtx;
    }


    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName)
            throws BeansException
    {
        if (bean instanceof IStatsCollectable)
        {
            logger.info("Inject statsCollector to " + bean.getClass());
            ((IStatsCollectable) bean).setStatsCollector(statsCollector);
        }

        if (bean instanceof IStatsCalculatorAware)
        {
            logger.info("Inject statsCalculator to " + bean.getClass());
            ((IStatsCalculatorAware) bean).setStatsCalculator(statsCalculator);
        }

        return bean;
    }


    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName)
            throws BeansException
    {
        return bean;
    }

    // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    class InjectorCachePair
    {
        IDataInjector injector;
        PositionCache positionCache;
        String initFromId;
    }


    public void setCacheName(String cacheName)
    {
        this.cacheName = cacheName;
    }

}
