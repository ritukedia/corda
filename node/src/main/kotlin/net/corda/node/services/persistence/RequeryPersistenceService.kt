package com.r3corda.node.services.schema

import com.r3corda.node.services.database.RequeryConfiguration
import net.corda.core.contracts.StateRef
import net.corda.core.schemas.MappedSchema
import net.corda.core.schemas.PersistentStateRef
import net.corda.core.schemas.QueryableState
import net.corda.core.utilities.loggerFor
import net.corda.node.services.api.SchemaService
import net.corda.node.services.persistence.AbstractPersistenceServiceImpl
import net.corda.node.services.vault.schemas.Models

/**
 * A general purpose service for Object Relational Mappings that are persisted with Requery.
 */
// TODO: Manage version evolution of the schemas via additional tooling.
class RequeryPersistenceService(override val schemaService: SchemaService) : AbstractPersistenceServiceImpl(schemaService) {

    private val configuration = RequeryConfiguration()

    companion object {
        val logger = loggerFor<RequeryPersistenceService>()
    }

    override fun persistStateWithSchema(state: QueryableState, stateRef: StateRef, schema: MappedSchema) {

        val session = configuration.sessionForModel(Models.DEFAULT)

        session.invoke {
            val mappedObject = schemaService.generateMappedObject(state, schema)
            mappedObject.stateRef = PersistentStateRef(stateRef)
            insert(mappedObject)
        }
    }

}

