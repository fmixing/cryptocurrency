import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.scheduling.*

@InternalCoroutinesApi
private val context = ExperimentalCoroutineDispatcher(corePoolSize = 2, maxPoolSize = 2)

data class Message(val transactionInfo: TransactionInfo, val deps: Set<TransactionInfo>)

interface ChannelMessage {
    val process: ProcessId
    val message: Message
}

data class BcastId(private val sender: ProcessId, private val transferId: Int)

/**
 * Адаптированный бродкаст из статьи Garcı́a-Pérez Á., Gotsman A., Federated Byzantine Quorum Systems
 * https://arxiv.org/pdf/1811.03642.pdf.
 * В алгоритм были добавлены экземпляры бродкаста [BcastId], которые включают в себя идентификатор транзакции. Это позволяет
 * "переплетенным" процессам (процессам, чьи кворумы пересекаются по корректным процессам) не принимать
 * в разных экземплярах бродкаста транзакции с одинаковым идентификатором.
 */
class Broadcast(private val processId: ProcessId,
                private val processChannel: Channel<ChannelMessage>,
                private val quorumSystem: FBQS,
                private val channels: Map<ProcessId, Channel<ChannelMessage>>,
                private val deliver: suspend (Message) -> Unit) {
    /**
     * Таблица, хранящая информацию, было ли отправлено сообщение [Echo] в экземпляре бродкаста [BcastId]
     */
    private val echoed : MutableMap<BcastId, Boolean> = HashMap()

    /**
     * Таблица, хранящая информацию, было ли отправлено сообщение [Ready] в экземпляре бродкаста [BcastId]
     */
    private val readied : MutableMap<BcastId, Boolean> = HashMap()

    /**
     * Таблица, хранящая информацию, ыло ли доставлено какое-то сообщение в экземпляре бродкаста [BcastId]
     */
    private val delivered : MutableMap<BcastId, Boolean> = HashMap()

    /**
     * Таблица, хранящая информацию, какие сообщения [Echo] были получены от каких процессов в экземпляре бродкаста [BcastId]
     */
    private val receivedEcho : MutableMap<BcastId, MutableMap<Message, MutableSet<ProcessId>>> = HashMap()

    /**
     * Таблица, хранящая информацию, какие сообщения [Ready] были получены от каких процессов в экземпляре бродкаста [BcastId]
     */
    private val receivedReady : MutableMap<BcastId, MutableMap<Message, MutableSet<ProcessId>>> = HashMap()

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
        val broadcastId = BcastId(cm.message.sender(), cm.message.transferId())
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

        val echoedProcess: MutableSet<ProcessId> = receivedEcho.getSafely(id).getSafely(m.message)
        echoedProcess += m.process
        val hasQuorum = quorumSystem.hasQuorum(processId, echoedProcess)
        if (hasQuorum) {
            readied[id] = true
            channels.values.forEach { it.send(Ready(processId, m.message)) }
        }
    }

    private suspend fun processReady(id: BcastId, m: ChannelMessage) {
        if (delivered.getOrElse(id) { false }) return

        val readiedProcess: MutableSet<ProcessId> = receivedReady.getSafely(id).getSafely(m.message)
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

data class Bcast(override val process: ProcessId, override val message: Message): ChannelMessage

data class Echo(override val process: ProcessId, override val message: Message): ChannelMessage

data class Ready(override val process: ProcessId, override val message: Message): ChannelMessage

private fun <Key, T> MutableMap<Key, MutableSet<T>>.getSafely(key: Key) = getOrPut(key) { HashSet() }

private fun <Key1, Key2, T> MutableMap<Key1, MutableMap<Key2, T>>.getSafely(key: Key1) = getOrPut(key) { HashMap() }

internal fun Message.sender() = transactionInfo.sender

internal fun Message.receiver() = transactionInfo.receiver

internal fun Message.transferId() = transactionInfo.transferId

internal fun Message.transferValue() = transactionInfo.transferValue