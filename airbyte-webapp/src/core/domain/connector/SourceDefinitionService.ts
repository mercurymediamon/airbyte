import { AirbyteRequestService } from "core/request/AirbyteRequestService";

import {
  createSourceDefinition,
  getSourceDefinition,
  listSourceDefinitionsForWorkspace,
  SourceDefinitionCreate,
  SourceDefinitionRead,
  SourceDefinitionUpdate,
  updateSourceDefinition,
} from "../../request/GeneratedApi";

export class SourceDefinitionService extends AirbyteRequestService {
  get url(): string {
    return "source_definitions";
  }

  public get(sourceDefinitionId: string) {
    return getSourceDefinition({ sourceDefinitionId });
  }

  public list(workspaceId: string) {
    return listSourceDefinitionsForWorkspace({ workspaceId });
  }

  public listLatest(workspaceId: string): Promise<{ sourceDefinitions: SourceDefinitionRead[] }> {
    // TODO: list_latest does not take a workspaceId
    return this.fetch(`${this.url}/list_latest`, {
      workspaceId,
    });
  }

  public update(body: SourceDefinitionUpdate) {
    return updateSourceDefinition(body);
  }

  public create(body: SourceDefinitionCreate) {
    return createSourceDefinition(body);
  }
}
