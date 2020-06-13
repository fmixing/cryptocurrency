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

/**
 * Реализация алгоритма перевода активов (алгоритма криптовалюты) без реализации протокола консенсуса из статьи
 * R. Guerraoui, P. Kuznetsov, M.Monti, M. Pavlovič, D.-A. Seredinschi, The Consensus Number of a Cryptocurrency,
 * https://arxiv.org/pdf/1906.05574.pdf.
 *
 * Все процессы видят какое-то состояние системы, и основываясь на этом, принимают решения, принимать ли очередную транзакцию
 * или нет. Было показано, что для симметричной системы кворумов отсутствует двойное расходование.
 * В моей работе рассматривается эта задача (задача криптовалюты) в асимметричных системах кворумов.
 */
class Cryptocurrency(private val ds: DistributedSystem,
                     val process: processId) {
    private val broadcast : Broadcast = Broadcast(process, ds.getChannel(process), ds.getProcessQuorumSystem(process),
            ds.getChannels(), this::deliver)

    /**
     * Количество провалидированных процессом [processMessage] исходящих транзакций каждого из процессов системы.
     */
    private val seq: IntArray = IntArray(ds.getProcesses().size)

    /**
     * Количество транзакций каждого из процессов системы, которые процесс [processMessage] получил в процессе бродкаста.
     */
    private val rec: IntArray = IntArray(ds.getProcesses().size)

    /**
     * Множество провалидированных процессом [processMessage] входящих и исходящих транзакций.
     */
    private val hist: ConcurrentMap<processId, MutableSet<TransactionInfo>> = ConcurrentHashMap()

    /**
     * Множество входящих транзакций процесса [processMessage] с момента прошлого успешного перевода денег через операцию [transfer].
     */
    private val deps: MutableSet<TransactionInfo> = ConcurrentHashMap.newKeySet()

    /**
     * Множество непровалидированных транзакций, которые процесс [processMessage] получил в процессе бродкаста.
     */
    private val toValidate: MutableSet<Message> = ConcurrentHashMap.newKeySet()

    /**
     * Сообщения от каждого из процессов системы, которые процесс [processMessage] должен обработать в порядке source order
     * (то есть все процессы системы обязаны их обработать в одном и том же порядке).
     */
    private val messageToDeliver: ConcurrentMap<processId, TreeSet<Message>> = ConcurrentHashMap()

    /**
     * Мьютекс, играющий роль CountDownLatch: он позволяет сообщать из корутины [validatingCoroutine], что перевод
     * процесса [processMessage] завершился успешно.
     */
    private var waitingTransactionCompletion = Mutex(true)

    /**
     * Канал для оповещения о том, что было получено новое сообщение, для которого необходимо выполнить валидацию.
     * Играет роль блокирующей очереди.
     */
    private val messagesToValidate = Channel<Message>(Channel.UNLIMITED)

    /**
     * Мьютекс, дающий гарантию, что корректные процессы не будут выполнять одновременно несколько операций перевода денег,
     * то есть что процессы, использующие криптовалюту – "однопоточные".
     */
    private val mutex = Mutex()

    @ExperimentalCoroutinesApi
    @InternalCoroutinesApi
    private val validatingCoroutine = CoroutineScope(context).launch { processToValidate() }

    suspend fun read(process: processId): Int = mutex.withLock { balance(process) }

    suspend fun transfer(receiver: processId, transferValue: Int) : Boolean = mutex.withLock {
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
            val received = messagesToValidate.receive()
            toValidate += received
            val iterator = toValidate.iterator()
            while (iterator.hasNext()) {
                if (processMessage(iterator.next())) iterator.remove()
            }
        }
    }

    private fun processMessage(m: Message): Boolean {
        val validated = validate(m)
        if (!validated) return false

        hist.getOrPut(m.sender) { HashSet() } += m.deps
        hist.getOrPut(m.sender) { HashSet() } += m.transactionInfo
        seq[m.sender] = m.transferId
        hist.getOrPut(m.receiver) { HashSet() } += m.transactionInfo

        if (process == m.receiver) {
            deps += m.transactionInfo
        }
        if (process == m.sender) {
            waitingTransactionCompletion.unlock()
        }
        return true
    }

    private fun validate(m: Message): Boolean {
        val hist = hist.getOrPut(m.sender) { HashSet() }
        return seq[m.sender] + 1 == m.transferId // эта транзакция следующая за той, которую процесс уже провалидировал
                && balance(m.sender) >= m.transferValue // на аккаунте процесса хватает денег
                && m.deps.map { hist.contains(it) }.all{it} // все зависимости транзакции процесс уже провалидировал
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
        return ds.getBalance(process) + incoming - outgoing
    }
}