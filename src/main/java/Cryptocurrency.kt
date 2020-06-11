import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*
import kotlinx.coroutines.sync.*
import java.util.*
import java.util.concurrent.*
import kotlin.collections.HashSet

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

class TransactionInfo(val sender: processId, val receiver: processId, val transferValue: money, val transferId: Int)

class Cryptocurrency(private val distributedSystem: DistributedSystem,
                     private val process: processId) {
    private val broadcast : Broadcast = Broadcast(distributedSystem.getChannels(),
            distributedSystem.getChannels()[process]!!, process, distributedSystem.getQuorumSystem()[process]!!, this::deliver)

    private val seq: IntArray = IntArray(distributedSystem.getProcesses().size)

    private val rec: IntArray = IntArray(distributedSystem.getProcesses().size)

    private val hist: ConcurrentMap<processId, MutableSet<TransactionInfo>> = ConcurrentHashMap()

    private val messageToDeliver: ConcurrentMap<processId, TreeSet<Message>> = ConcurrentHashMap();

    @OptIn(ExperimentalStdlibApi::class)
    private val deps: MutableSet<TransactionInfo> = HashSet()

    @OptIn(ExperimentalStdlibApi::class)
    private val toValidate: MutableSet<Message> = HashSet()

    private var waitingTransactionCompletion = Mutex(true)

    private val messagesToValidate = Channel<Message>(Channel.UNLIMITED)

    private val mutex = Mutex()

    @ExperimentalCoroutinesApi
    @InternalCoroutinesApi
    private val runCoroutine = CoroutineScope(context).launch { processToValidate() }

    public suspend fun read(process: processId): Int = mutex.withLock { balance(process) }

    public suspend fun transfer(receiver: processId, transferValue: Int) : Boolean = mutex.withLock {
        if (balance(process) < transferValue) {
            return false
        }

        val transactionInfo = TransactionInfo(process, receiver, transferValue, seq[process] + 1)
        waitingTransactionCompletion = Mutex(true)
        broadcast.broadcast(Message(transactionInfo, deps.toSet()))
        deps.clear()
        waitingTransactionCompletion.lock()

        return true
    }

    private suspend fun deliver(m: Message) {
        val messages = messageToDeliver.getOrPut(m.sender) { TreeSet(compareBy { it.transferId }) }
        messages += m

        val iterator = messages.iterator()
        var firstMessage = iterator.next()

        while (firstMessage.transferId == rec[m.sender] + 1) {
            rec[m.sender] += 1
            messagesToValidate.send(m)

            iterator.remove()
            if (!iterator.hasNext()) return
            firstMessage = iterator.next()
        }
    }

    @ExperimentalCoroutinesApi
    private suspend fun processToValidate() {
        while (true) {
            val received = messagesToValidate.receiveOrNull() ?: return
            toValidate += received
            val iterator = toValidate.iterator()
            while (iterator.hasNext()) {
                val m = iterator.next()
                val validated = validate(m)
                if (validated) {
                    hist[m.sender]!! += m.deps
                    hist[m.sender]!! += m.transactionInfo
                    seq[m.sender] = m.transferId
                    hist[m.receiver]!! += m.transactionInfo

                    if (process == m.receiver) {
                        deps += m.transactionInfo
                    }
                    if (process == m.sender) {
                        waitingTransactionCompletion.unlock()
                    }
                    iterator.remove()
                }
            }
        }
    }

    private fun validate(next: Message): Boolean {
        val hist = hist.getOrPut(next.sender) { HashSet() }
        return seq[next.sender] == next.transferId + 1
                && balance(next.sender) > next.transferValue
                && next.deps.map { hist.contains(it) }.all{it}
    }


    private fun balance(process: processId): Int {
        val hist = hist.getOrPut(process) { HashSet() }
        val outgoing = hist.stream()
                .filter { info -> info.sender == process }
                .mapToInt { it.transferValue }
                .sum()
        val incoming = hist.stream()
                .filter { info -> info.receiver == process }
                .mapToInt { it.transferValue }
                .sum()
        return distributedSystem.getBalances()[process]!! + incoming - outgoing
    }
}