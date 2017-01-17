package kohl.hadrien.vtl.connector;

/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */


import kohl.hadrien.vtl.model.Dataset;

/**
 * Interface that allows the dataset
 */
public interface Connector {

    boolean canHandle(String identifier);

    Dataset getDataset(String identifier) throws ConnectorException;

    Dataset putDataset(String identifier, Dataset dataset) throws ConnectorException;

}
