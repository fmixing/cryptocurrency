import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

/**
 * Адаптированный бродкаст из статьи Garcı́a-Pérez Á., Gotsman A., Federated Byzantine Quorum Systems
 * https://arxiv.org/pdf/1811.03642.pdf.
 * В алгоритм были добавлены процессы бродкаста [BcastId], которые включают в себя идентификатор транзакции. Это позволяет
 * "переплетенным" процессам (процессам, чьи кворумы пересекаются по корректным процессам) не принимать
 * в разных процессах бродкаста транзакции с одинаковым идентификатором.
 */
class Broadcast(private val processId: processId,
                private val processChannel: Channel<ChannelMessage>,
                private val quorumSystem: FBQS,
                private val channels: Map<processId, Channel<ChannelMessage>>,
                private val deliver: suspend (Message) -> Unit) {
    /**
     * Таблица, хранящая информацию, было ли отправлено сообщение [Echo] в процессе бродкаста [BcastId]
     */
    private val echoed : MutableMap<BcastId, Boolean> = HashMap()

    /**
     * Таблица, хранящая информацию, было ли отправлено сообщение [Ready] в процессе бродкаста [BcastId]
     */
    private val readied : MutableMap<BcastId, Boolean> = HashMap()

    /**
     * Таблица, хранящая информацию, ыло ли доставлено какое-то сообщение в процессе бродкаста [BcastId]
     */
    private val delivered : MutableMap<BcastId, Boolean> = HashMap()

    /**
     * Таблица, хранящая информацию, какие сообщения [Echo] были получены от каких процессов в процессе бродкаста [BcastId]
     */
    private val receivedEcho : MutableMap<BcastId, MutableMap<Message, MutableSet<processId>>> = HashMap()

    /**
     * Таблица, хранящая информацию, какие сообщения [Ready] были получены от каких процессов в процессе бродкаста [BcastId]
     */
    private val receivedReady : MutableMap<BcastId, MutableMap<Message, MutableSet<processId>>> = HashMap()

    @ExperimentalCoroutinesApi
    @InternalCoroutinesApi
    private val runCoroutine = CoroutineScope(context).launch { receive() }

    suspend fun broadcast(m: Message) {
        channels.values.forEach { it.send(Bcast(processId, m)) }
    }

    private suspend fun receive() {
        while (true) {
            val message = processChannel.receive()
            process(message)
        }
    }

    private suspend fun process(cm: ChannelMessage) {
        val broadcastId = BcastId(cm.message.sender, cm.message.transferId)
        when (cm) {
            is Bcast -> processBcast(broadcastId, cm)
            is Echo -> processEcho(broadcastId, cm)
            is Ready -> processReady(broadcastId, cm)
        }
    }

    private suspend fun processBcast(id: BcastId, m: ChannelMessage) {
        if (!echoed.getOrElse(id) { false }) {
            echoed[id] = true
            channels.values.forEach { it.send(Echo(processId, m.message)) }
        }
    }

    private suspend fun processEcho(id: BcastId, m: ChannelMessage) {
        if (readied.getOrElse(id) { false }) return

        val echoedProcess: MutableSet<processId> = receivedEcho.getOrPut(id){ HashMap() }.getOrPut(m.message){ HashSet() }
        echoedProcess += m.process
        val hasQuorum = quorumSystem.hasQuorum(processId, echoedProcess)
        if (hasQuorum) {
            readied[id] = true
            channels.values.forEach { it.send(Ready(processId, m.message)) }
        }
    }

    private suspend fun processReady(id: BcastId, m: ChannelMessage) {
        if (delivered.getOrElse(id) { false }) return

        val readiedProcess: MutableSet<processId> = receivedReady.getOrPut(id){ HashMap() }.getOrPut(m.message){ HashSet() }
        readiedProcess += m.process
        val hasQuorum = quorumSystem.hasQuorum(processId, readiedProcess)
        if (hasQuorum) {
            delivered[id] = true
            deliver.invoke(m.message)
            return
        }

        val hasBlockingSet = quorumSystem.hasBlockingSet(processId, readiedProcess)
        if (hasBlockingSet && !readied.getOrElse(id) { false }) {
            readied[id] = true
            channels.values.forEach { it.send(Ready(processId, m.message)) }
        }
    }
}

open class ChannelMessage(val process: processId, val message: Message)

class Bcast(process: processId, m: Message): ChannelMessage(process, m)

class Echo(process: processId, m: Message): ChannelMessage(process, m)

class Ready(process: processId, m: Message): ChannelMessage(process, m)

class Message(val transactionInfo: TransactionInfo, val deps: Set<TransactionInfo>) {
    val sender = transactionInfo.sender
    val receiver = transactionInfo.receiver
    val transferId = transactionInfo.transferId
    val transferValue = transactionInfo.transferValue

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Message

        if (deps != other.deps) return false
        if (sender != other.sender) return false
        if (receiver != other.receiver) return false
        if (transferId != other.transferId) return false
        if (transferValue != other.transferValue) return false

        return true
    }

    override fun hashCode(): Int {
        var result = deps.hashCode()
        result = 31 * result + sender
        result = 31 * result + receiver
        result = 31 * result + transferId
        result = 31 * result + transferValue
        return result
    }
}

private class BcastId(private val sender: processId, private val transferId: Int) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as BcastId

        if (sender != other.sender) return false
        if (transferId != other.transferId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sender
        result = 31 * result + transferId
        return result
    }
}
