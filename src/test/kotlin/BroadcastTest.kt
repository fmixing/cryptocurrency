import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.sync.*
import org.junit.jupiter.api.*

@InternalCoroutinesApi
class BroadcastTest {
    /**
     * Тест, что если процесс отправил сообщение, то он его доставит, если все остальные процессы корректные.
     */
    @Test
    fun testDeliveryWhenBroadcast() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val mutex = Mutex(true)
        val broadcast = Broadcast(0, ds.getChannel(0), ds.getProcessQuorumSystem(0),
                ds.getChannels()) { mutex.unlock() }
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        broadcast.broadcast(Message(TransactionInfo(0, 1, 10, 1), setOf()))
        mutex.lock()
    }

    /**
     * Тест, что если процесс получил сообщение через бродкаст, то он его доставит, если все остальные процессы корректные.
     */
    @Test
    fun testDeliveryWhenBroadcasted() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val mutex = Mutex(true)
        Broadcast(0, ds.getChannel(0), ds.getProcessQuorumSystem(0), ds.getChannels()) { mutex.unlock() }
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        ds.getChannel(0).send(Bcast(1, Message(TransactionInfo(1, 0, 10, 1), setOf())))
        mutex.lock()
    }

    /**
     * Тест, что если процесс доставил сообщение в одном процессе бродкаста, он не доставит другое сообщение
     * в этом процессе бродкаста.
     */
    @Test
    fun testNoDuplication() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val mutex = Mutex(true)
        val delivered = HashSet<Message>()
        Broadcast(0, ds.getChannel(0), ds.getProcessQuorumSystem(0), ds.getChannels()) { m ->
            delivered += m
            mutex.unlock()
        }
        GlobalScope.launch {
            while (true) {
                select<Unit> {
                    ds.getChannel(1).onReceive { sendAnswers(1, it, ds.getChannel(0)) }
                    ds.getChannel(2).onReceive { sendAnswers(2, it, ds.getChannel(0)) }
                }
            }
        }
        ds.getChannel(0).send(Bcast(1, Message(TransactionInfo(1, 0, 10, 1), setOf())))
        mutex.lock()
        val m = Message(TransactionInfo(1, 2, 10, 1), setOf())
        ds.getChannel(0).send(Bcast(1, m))
        ds.getChannel(0).send(Echo(1, m))
        ds.getChannel(0).send(Echo(2, m))
        ds.getChannel(0).send(Ready(1, m))
        ds.getChannel(0).send(Ready(2, m))
        delay(10)
        check(!delivered.contains(m))
    }

    /**
     * Проверка, что если процесс получил Ready от некоторого блокирующего множества, то он отправит Ready (а так как
     * после этого образуется кворум, то он и доставит сообщение)
     */
    @Test
    fun testBlockingSetDelivered() = runBlocking {
        val ds = MajoritySymmetricDistributedSystem(3)
        val mutex = Mutex(true)
        Broadcast(0, ds.getChannel(0), ds.getProcessQuorumSystem(0), ds.getChannels()) { mutex.unlock() }
        ds.getChannel(0).send(Ready(1, Message(TransactionInfo(1, 0, 10, 1), setOf())))
        ds.getChannel(0).send(Ready(2, Message(TransactionInfo(1, 0, 10, 1), setOf())))
        mutex.lock()
    }

    /**
     * Проверка, то нетронутое множество обладает тотальностью, и не примет два разных сообщения.
     */
    @Test
    fun testTotalityAndValidity() = runBlocking {
        val ds = SymmetricDistributedSystemWithoutQI()
        val delivered = HashSet<Message>()
        val mutex: Pair<Mutex, Mutex> = createIntactSets(0, ds, delivered)
        sendMaliciousMessages(1, ds, false)
        delay(10)
        if (delivered.size == 1) {
            mutex.first.lock()
            mutex.second.lock()
        }
        else {
            check(delivered.size == 0)
        }
    }

    /**
     * Проверка, то нетронутое множество обладает тотальностью, и не примет два разных сообщения, но при этом
     * разные нетронутые множества могут доставить разные сообщения.
     */
    @Test
    fun testTotalityAndValidityForMultipleIntactSets() = runBlocking {
        val intactSetNumber = 4
        val ds = SymmetricDistributedSystemWithoutQI(intactSetNumber)
        val delivered = HashMap<Int, MutableSet<Message>>()
        val mutex = ArrayList<Pair<Mutex, Mutex>>()
        repeat(intactSetNumber) {
            mutex += createIntactSets(it, ds, delivered.getOrPut(it) { HashSet() })
        }
        sendMaliciousMessages(intactSetNumber, ds, true)
        delay(100)
        repeat(intactSetNumber) {
            if (!mutex[it].first.isLocked || !mutex[it].second.isLocked) {
                mutex[it].first.lock()
                mutex[it].second.lock()
                check(delivered[it]!!.size == 1)
            }
        }
    }

    private suspend fun sendAnswers(process: processId, m: ChannelMessage, c: Channel<ChannelMessage>) {
        when (m) {
            is Bcast -> c.send(Echo(process, m.message))
            is Echo -> c.send(Ready(process, m.message))
        }
    }

    private fun createIntactSets(intactSetId: Int, ds: DistributedSystem<ChannelMessage>, delivered: MutableSet<Message>): Pair<Mutex, Mutex> {
        val mutex1 = Mutex(true)
        val mutex2 = Mutex(true)
        val processId1 = intactSetId * 2
        val processId2 = intactSetId * 2 + 1
        Broadcast(processId1, ds.getChannel(processId1), ds.getProcessQuorumSystem(processId1), ds.getChannels()) { m ->
            delivered += m
            mutex1.unlock()
        }
        Broadcast(processId2, ds.getChannel(processId2), ds.getProcessQuorumSystem(processId2), ds.getChannels()) { m ->
            delivered += m
            mutex2.unlock()
        }
        return Pair(mutex1, mutex2)
    }
}

suspend fun sendMaliciousMessages(intactSetNumber: Int, ds: DistributedSystem<ChannelMessage>, sameMessagesForIntactSet: Boolean) {
    val same = if (sameMessagesForIntactSet) 0 else 1
    val maliciousProcessId = intactSetNumber * 2
    repeat(intactSetNumber) {
        ds.getChannel(it * 2).send(Bcast(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2, 10, 1), setOf())))
        ds.getChannel(it * 2 + 1).send(Bcast(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2 + same, 10, 1), setOf())))
    }
    repeat(intactSetNumber) {
        ds.getChannel(it * 2).send(Echo(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2, 10, 1), setOf())))
        ds.getChannel(it * 2 + 1).send(Echo(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2 + same, 10, 1), setOf())))
    }
    repeat(intactSetNumber) {
        ds.getChannel(it * 2).send(Ready(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2, 10, 1), setOf())))
        ds.getChannel(it * 2 + 1).send(Ready(maliciousProcessId,
                Message(TransactionInfo(maliciousProcessId, it * 2 + same, 10, 1), setOf())))
    }
}

/**
 * Симметричная система кворумов без требования о пересечении кворумов.
 */
internal class SymmetricDistributedSystemWithoutQI(private val intactSets: Int = 1) : DistributedSystem<ChannelMessage> {
    private val channels: MutableMap<processId, Channel<ChannelMessage>> = HashMap()
    private val qs: MutableMap<processId, FBQS> = HashMap()
    private val balances: MutableMap<processId, money> = HashMap()

    init {
        repeat(intactSets * 2) {
            channels[it] = Channel(Channel.UNLIMITED)
        }
        channels[intactSets * 2] = Channel(Channel.UNLIMITED)
        repeat(intactSets * 2) {
            qs[it] = SymmetricFBQS()
        }
        qs[intactSets * 2] = SymmetricFBQS()
        repeat(intactSets * 2) {
            balances[it] = 100
        }
        balances[intactSets * 2] = 100
    }

    override fun getProcesses(): Set<processId> = channels.keys

    override fun getChannels(): Map<processId, Channel<ChannelMessage>> = channels

    override fun getQuorumSystem(): Map<processId, FBQS> = qs

    override fun getBalances(): Map<processId, money> = balances

    private inner class SymmetricFBQS : FBQS {
        override fun hasQuorum(process: processId, processes: Set<processId>): Boolean {
            var hasQuorum = false
            repeat(intactSets) {
                hasQuorum = hasQuorum || processes.containsAll(setOf(it * 2, it * 2 + 1)) && (process == it * 2 || process == it * 2 + 1)
            }
            return hasQuorum
        }

        override fun hasBlockingSet(process: processId, processes: Set<processId>): Boolean {
            var hasBlockingSet = false
            repeat(intactSets) {
                hasBlockingSet = hasBlockingSet || processes.contains(it * 2) && process == it * 2 + 1 ||
                        processes.contains(it * 2 + 1) && process == it * 2
            }
            return hasBlockingSet
        }
    }
}