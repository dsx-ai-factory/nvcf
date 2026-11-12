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

package com.nvidia.apikeys.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.nvidia.apikeys.config.NakProperties;
import com.nvidia.apikeys.vo.KeyVo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

class KeyNcaIdResolverTest {

    private final KeyNcaIdResolver resolver =
            new KeyNcaIdResolver(NakProperties.builder().ncaId("config-nca").build());

    @ParameterizedTest
    @CsvSource(
            value = {
                "null,null,config-nca",
                "column-nca,null,column-nca",
                "null,details-nca,details-nca",
                "same-nca,same-nca,same-nca",
                "'  ',null,config-nca"
            },
            nullValues = "null")
    void resolve(String column, String details, String expected) {
        var key = KeyVo.builder().ncaId(column).ncaIdInDetails(details).build();

        assertEquals(expected, resolver.resolve(key));
    }

    @Test
    void resolve_failsWhenColumnAndDetailsDiffer() {
        var key = KeyVo.builder().ncaId("column-nca").ncaIdInDetails("details-nca").build();

        assertThrows(IllegalStateException.class, () -> resolver.resolve(key));
    }
}
