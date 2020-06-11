interface FBQS {
    /**
     * Проверяет, есть ли в множестве процессов [processes] кворум указанного процесса [process]
     */
    fun hasQuorum(process: processId, processes: Set<processId>) : Boolean

    /**
     * Проверяет, есть ли в множестве процессов [processes] блокирующее множество указанного процесса [process]
     */
    fun hasBlockingSet(process: processId, processes: Set<processId>) : Boolean
}

typealias processId = Int

typealias money = Int