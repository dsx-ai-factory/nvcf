/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.apikeys.persistance.repositories;

import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_CREATED_AT;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_DELETES_AT;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_EXPIRES_AT;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_ISSUER_SERVICE_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_DETAILS;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_KEY_STATUS;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_NCA_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_OWNER_ID;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.COLUMN_OWNER_TYPE;
import static com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel.TABLE_NAME;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.nvidia.apikeys.persistance.models.KeyByOwnerAndAccountAndServiceModel;
import com.nvidia.apikeys.vo.KeyOwnerType;
import java.util.List;
import java.util.Optional;
import org.springframework.data.cassandra.repository.Consistency;
import org.springframework.data.cassandra.repository.MapIdCassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface KeyByOwnerAndAccountAndServiceRepository extends
        MapIdCassandraRepository<KeyByOwnerAndAccountAndServiceModel> {

    List<KeyByOwnerAndAccountAndServiceModel> findByNcaIdAndOwnerTypeAndOwnerId(
            String ncaId, KeyOwnerType ownerType, String ownerId);

    List<KeyByOwnerAndAccountAndServiceModel> findByNcaIdAndOwnerTypeAndOwnerIdAndIssuerServiceId(
            String ncaId, KeyOwnerType ownerType, String ownerId, String issuerServiceId);

    @Query("SELECT * FROM " + TABLE_NAME
            + " WHERE " + COLUMN_NCA_ID + "=:ncaId"
            + " AND " + COLUMN_OWNER_TYPE + "=:ownerType"
            + " AND " + COLUMN_OWNER_ID + "=:ownerId"
            + " AND " + COLUMN_ISSUER_SERVICE_ID + "=:issuerServiceId"
            + " AND " + COLUMN_KEY_ID + "=:keyId")
    Optional<KeyByOwnerAndAccountAndServiceModel> findKey(
            @Param("ncaId") String ncaId, @Param("ownerType") KeyOwnerType ownerType,
            @Param("ownerId") String ownerId, @Param("issuerServiceId") String issuerServiceId,
            @Param("keyId") String keyId);

    @Consistency(DefaultConsistencyLevel.LOCAL_QUORUM)
    @Query("SELECT " + COLUMN_NCA_ID
            + ", " + COLUMN_OWNER_TYPE
            + ", " + COLUMN_OWNER_ID
            + ", " + COLUMN_ISSUER_SERVICE_ID
            + ", " + COLUMN_KEY_ID
            + ", " + COLUMN_KEY_STATUS
            + ", " + COLUMN_CREATED_AT
            + ", " + COLUMN_EXPIRES_AT
            + ", " + COLUMN_DELETES_AT
            + ", " + COLUMN_KEY_DETAILS
            + ", WRITETIME(" + COLUMN_KEY_STATUS + ") AS wt_" + COLUMN_KEY_STATUS
            + ", WRITETIME(" + COLUMN_CREATED_AT + ") AS wt_" + COLUMN_CREATED_AT
            + ", WRITETIME(" + COLUMN_EXPIRES_AT + ") AS wt_" + COLUMN_EXPIRES_AT
            + ", WRITETIME(" + COLUMN_DELETES_AT + ") AS wt_" + COLUMN_DELETES_AT
            + ", WRITETIME(" + COLUMN_KEY_DETAILS + ") AS wt_" + COLUMN_KEY_DETAILS
            + " FROM " + TABLE_NAME
            + " WHERE " + COLUMN_NCA_ID + "=:ncaId"
            + " AND " + COLUMN_OWNER_TYPE + "=:ownerType"
            + " AND " + COLUMN_OWNER_ID + "=:ownerId"
            + " AND " + COLUMN_ISSUER_SERVICE_ID + "=:issuerServiceId"
            + " AND " + COLUMN_KEY_ID + "=:keyId")
    ResultSet findMigrationRow(@Param("ncaId") String ncaId,
            @Param("ownerType") KeyOwnerType ownerType, @Param("ownerId") String ownerId,
            @Param("issuerServiceId") String issuerServiceId, @Param("keyId") String keyId);
}
