package net.corda.core.schemas

import io.requery.*

object Requery {
    /**
     * A super class for all mapped states exported to a schema that ensures the [StateRef] appears on the database row.  The
     * [StateRef] will be set to the correct value by the framework (there's no need to set during mapping generation by the state itself).
     */
    @Superclass interface PersistentState : Persistable {
        @get:Key
        @get:Column(name = "transaction_id", length = 64)
        var txId: String?

        @get:Key
        @get:Column(name = "output_index")
        var index: Int?
    }
}
