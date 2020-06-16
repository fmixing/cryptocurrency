typealias ProcessId = Int
typealias Money = Int

interface DistributedSystem<Channel> {
    /**
     * Получение множества прооцессов распределенной системы.
     */
    fun getProcesses() : Set<ProcessId>

    /**
     * "Надежные" каналы связи, по которым процессы могут слать друг другу сообщения.
     */
    fun getChannels() : Map<ProcessId, Channel>

    /**
     * Асимметричная система кворумов, в ней для каждого процесса задается своя система кворумов.
     */
    fun getQuorumSystem() : Map<ProcessId, FBQS>

    /**
     * Исходные балансы процессов.
     */
    fun getBalances() : Map<ProcessId, Money>
}

fun <Channel> DistributedSystem<Channel>.getChannel(process: ProcessId) = getChannels()[process]
        ?: error("Process $process doesn't exist")

fun <Channel> DistributedSystem<Channel>.getBalance(process: ProcessId) = getBalances()[process]
        ?: error("Process $process doesn't exist")

fun <Channel> DistributedSystem<Channel>.getProcessQuorumSystem(process: ProcessId) = getQuorumSystem()[process]
        ?: error("Process $process doesn't exist")