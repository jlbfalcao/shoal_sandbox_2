import com.sun.enterprise.ee.cms.core.*;
import com.sun.enterprise.ee.cms.impl.client.*;
import com.sun.enterprise.ee.cms.impl.common.FailureRecoverySignalImpl;
import com.sun.enterprise.ee.cms.impl.common.GroupLeadershipNotificationSignalImpl;
import com.sun.enterprise.ee.cms.impl.common.JoinNotificationSignalImpl;
import com.sun.enterprise.ee.cms.spi.MemberStates;
import com.sun.enterprise.jxtamgmt.JxtaUtil;

import java.text.MessageFormat;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.sun.enterprise.ee.cms.core.ServiceProviderConfigurationKeys.FAILURE_DETECTION_TIMEOUT;
import static com.sun.enterprise.ee.cms.core.ServiceProviderConfigurationKeys.FAILURE_VERIFICATION_TIMEOUT;

/**
 * A Simple sample client application that uses Shoal library to register for
 * group events, join a pre-defined group,  get notifications of group events,
 * send/receive messages, and leave the group.
 * To see Shoal functionality, run this exmaple in two or more terminals or
 * machines on the same subnet (Cross subnet functionality will be available in
 * about a month end of August 07)
 */


public class SimpleGMSSample implements CallBack {
    final static Logger logger = Logger.getLogger("SimpleGMSSample");
    final Object waitLock = new Object();

    private String serverName;
    private GroupManagementService gms;

    private Boolean reallyReady = false;

    public static void main(String[] args) {


        JxtaUtil.setupLogHandler();


        SimpleGMSSample sgs = new SimpleGMSSample();
        try {
            sgs.runSimpleSample();
        } catch (GMSException e) {
            logger.log(Level.SEVERE, "Exception occured while joining group:" + e);
        }
    }


    public class CLB implements Runnable {
        boolean getMemberState;
        long threshold;
        long timeout;

        public CLB(boolean getMemberState, long threshold, long timeout) {
            this.getMemberState = getMemberState;
            this.threshold = threshold;
            this.timeout = timeout;
        }

        private void getAllMemberStates() {
            long startTime = System.currentTimeMillis();
            List<String> members = gms.getGroupHandle().getCurrentCoreMembers();
//            logger.info("Enter getAllMemberStates currentMembers=" + members.size() + " threshold(ms)=" + threshold +
//                    " timeout(ms)=" + timeout);
            for (String member : members) {
                MemberStates state = gms.getGroupHandle().getMemberState(member, threshold, timeout);
                logger.info("getMemberState member=" + member + " state=" + state +
                        " threshold=" + threshold + " timeout=" + timeout);
            }
//            logger.info("exit getAllMemberStates()  elapsed time=" + (System.currentTimeMillis() - startTime) +
//                    " ms " + "currentMembers#=" + members.size());
        }

        public void run() {
            while (getMemberState) { // && !stopped) {
                getAllMemberStates();
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ie) {
                }
            }
        }
    }

    /**
     * Runs this sample
     *
     * @throws GMSException
     */
    private void runSimpleSample() throws GMSException {
        logger.log(Level.INFO, "Starting SimpleGMSSample....");

        serverName = "server" + System.currentTimeMillis();


        System.out.println(" >>>>>>>>>>>>>>>>> EU SOU = " + serverName);
        final String groupName = "Group1";

//        gms.
//        gms.reportJoinedAndReadyState();
//        gms.addActionFactory(myfailureNotificationActionFactoryImpl);

        //initialize Group Management Service
        gms = initializeGMS(serverName, groupName);
        System.out.println("getMemberType=" + gms.getMemberType());
//        gms.announceGroupStartup();

        //register for Group Events
        registerForGroupEvents(gms);
        //join group

        joinGMSGroup(groupName, gms);

        try {
            //send some messages
//            sendMessages(gms, serverName);
//            Thread.sleep(5000);
            System.out.println("--------------");
            gms.reportJoinedAndReadyState(groupName);

            final Thread clbThread = new Thread(new CLB(true, 1000, 1000), "CLB");
            clbThread.start();


            waitForShutdown();

        } catch (InterruptedException e) {
            logger.log(Level.WARNING, e.getMessage());
        }
        //leave the group gracefully
        leaveGroupAndShutdown(serverName, gms);
    }

    private GroupManagementService initializeGMS(String serverName, String groupName) {
        logger.log(Level.INFO, "Initializing Shoal for member: " + serverName + " group:" + groupName);
//        GMSFactory.setGMSEnabledState(groupName, true);
        Properties config = new Properties();
//        config.put(FAILURE_DETECTION_TIMEOUT.name(), 1000);
//        config.put(FAILURE_VERIFICATION_TIMEOUT.name(), 1000);
//        config.put(ServiceProviderConfigurationKeys.BIND_INTERFACE_ADDRESS, System.getProperty("ip"));
        return (GroupManagementService) GMSFactory.startGMSModule(serverName,
                groupName, GroupManagementService.MemberType.CORE, null); //config);
    }

    private void registerForGroupEvents(GroupManagementService gms) {
        logger.log(Level.INFO, "Registering for group event notifications");
        gms.addActionFactory(new JoinNotificationActionFactoryImpl(this));
        gms.addActionFactory(new JoinedAndReadyNotificationActionFactoryImpl(this));
        gms.addActionFactory(new FailureSuspectedActionFactoryImpl(this));
        gms.addActionFactory("core", new FailureRecoveryActionFactoryImpl(this));
//        gms.addActionFactory(new FailureRecoveryActionFactoryImpl(this));
//        gms.addActionFactory(new FailureRecoveryActionFactoryImpl(this));
//        gms.addActionFactory(new PlannedShutdownActionFactoryImpl(this));
//        gms.addActionFactory(new MessageActionFactoryImpl(this), "SimpleSampleComponent");
        gms.addActionFactory(new GroupLeadershipNotificationActionFactoryImpl(this));
    }

    private void joinGMSGroup(String groupName, GroupManagementService gms) throws GMSException {
        logger.log(Level.INFO, "Joining Group " + groupName);
//        gms.getGroupHandle().
//        System.out.println("groupleader=" + gms.getGroupHandle().getGroupLeader());

        gms.join();
    }

    private void sendMessages(GroupManagementService gms, String serverName) throws InterruptedException, GMSException {
        logger.log(Level.INFO, "wait 15 secs to send 10 messages");
        synchronized (waitLock) {
            waitLock.wait(10000);
        }
        GroupHandle gh = gms.getGroupHandle();

        logger.log(Level.INFO, "Sending messages...");
        for (int i = 0; i <= 10; i++) {
            gh.sendMessage("SimpleSampleComponent",
                    MessageFormat.format("Message {0}from server {1}", i, serverName).getBytes());
        }
    }

    private void waitForShutdown() throws InterruptedException {
        logger.log(Level.INFO, "wait 20 secs to shutdown");
        synchronized (waitLock) {
            waitLock.wait(); //20000);
        }
    }

    private void leaveGroupAndShutdown(String serverName, GroupManagementService gms) {
        logger.log(Level.INFO, "Shutting down instance " + serverName);
        gms.shutdown(GMSConstants.shutdownType.INSTANCE_SHUTDOWN);
        System.exit(0);
    }

    public void processNotification(Signal signal) {
        System.out.println("Received Notification of type : " + signal.getClass().getName());

        if (signal instanceof FailureRecoverySignalImpl) {
            System.out.println("******************** FailureRecoverySignalImpl **********************");

            FailureRecoverySignal s = (FailureRecoverySignal) signal;

//                s.getComponentName()
            System.out.println("component-name=" + s.getComponentName());
            System.out.println("member-details=" + s.getMemberDetails());

            if (s.getMemberDetails().containsKey("master") && (Boolean) s.getMemberDetails().get("master")) {
                System.out.println("==============================\n SOU O MASTER (agora) \n================================\n");
                try {
                    gms.updateMemberDetails(serverName, "master", true);
                } catch (GMSException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

            }


            return;
        }


        logger.log(Level.INFO, "Received Notification of type : " + signal.getClass().getName());
        try {
            System.out.println("md=" + signal.getMemberDetails());
            logger.log(Level.INFO, "Source Member: " + signal.getMemberToken());

            signal.acquire();
//            logger.log(Level.INFO, "Source Member: " + signal.getMemberToken());

            if (signal instanceof JoinedAndReadyNotificationSignal) {
                System.out.println("******************** JoinedAndReadyNotificationSignal **********************");

                JoinedAndReadyNotificationSignal s = (JoinedAndReadyNotificationSignal) signal;
                GroupHandle groupHandle = gms.getGroupHandle();
                System.out.println("who?=" + s.getMemberToken());

//                groupHandle.getDistributedStateCache().addToCache("sdfasd", null.);

                // eu sou lider do cluster E esse evento é sobre "mim".
                if (groupHandle.getGroupLeader().equals(serverName) && s.getMemberToken().equals(serverName)) {
                    if (!gms.getMemberDetails(serverName).containsKey("master")) {
                        System.out.println("==============================\n SOU O MASTER \n================================\n");
                        try {
                            gms.updateMemberDetails(serverName, "master", true);
                        } catch (GMSException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }

            } else if (signal instanceof GroupLeadershipNotificationSignalImpl) {
            } else if (signal instanceof JoinNotificationSignalImpl) {
            }

            signal.release();
        } catch (SignalAcquireException e) {
            logger.log(Level.WARNING, "Exception occured while acquiring signal" + e);
        } catch (SignalReleaseException e) {
            logger.log(Level.WARNING, "Exception occured while releasing signal" + e);
        }

    }
}
